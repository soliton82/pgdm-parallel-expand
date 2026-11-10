# Parallel, resumable Expand — solution notes

Kaggle: *Python & PostgreSQL Data Pipeline Optimization Challenge*. Author: soliton82.
Licence: Apache-2.0 (same as the repository).

**In short:** the Expand operation now evaluates each recipe on all CPU cores of the
PostgreSQL server, then loads every phase's rows in **one transaction** that drops the
destination's foreign keys, unique constraints, secondary indexes and (unreferenced)
primary key, appends the rows, and re-creates and re-validates every one of them
**before the commit**. Every step commits together with a journal entry, so an
interrupted run resumes where it stopped. The final database is **byte-identical** to
the original engine's result, including every generated identity value, the sequence
states, the row counters, the version/dependency records and the schema.

| Local benchmark (MEASURED PROCESSING TOTAL) | original engine | this solution | speed-up |
|---|---:|---:|---:|
| 100,000 `raw_data` rows | 9.11 s | **2.23 s** | 4.1× |
| 1,000,000 `raw_data` rows | 90.8 s | **17.96 – 18.73 s** | 4.8 – 5.1× |

*These are local results, not official scores.* Hardware: Apple M3 Pro. The Linux
server is an aarch64 VM with 6 vCPUs and 8 GB RAM, running PostgreSQL 16 (Docker
`postgres:16`, `shared_buffers=1GB`). The DB Manager talks to it over SSH and `psql`,
exactly like the original architecture. Databases come from the official, unmodified
`generate_database.py`; `raw_data` was vacuumed as the generator's own autovacuum
settings do shortly after generation. The PostgreSQL role is a dedicated **non-superuser**
role that owns the database, as `GENERATE_DATABASE_README.md` prescribes. The host also
ran unrelated jobs, so the ranges show run-to-run spread.

---

## 1. Where the time went (original engine)

| 1M rows | seconds | why |
|---|---:|---|
| `INSERT … table2` | 50.1 | 14-way `LATERAL VALUES` fan-out + 2.3M rows with FK checks |
| `INSERT … table1` | 23.3 | jsonb extraction + 2 FKs + 2 unique indexes |
| `INSERT … table6` / `table5` | 11.9 / 11.0 | join with `table1`, FK checks |
| `table3` / `table4` (DISTINCT) | 2.6 / 2.3 | full scans |
| preflight (≈24 separate `psql` launches) | 0.7 | process start + connect + auth for each check |

All of this runs in **one backend on one CPU core**. PostgreSQL never parallelises
the `SELECT` of an `INSERT … SELECT`, so the server's other cores sit idle. Each of the
7.3 million inserted rows also fires its foreign-key triggers one by one; the
`table1 → raw_data` check takes a row lock on the 2 GB source table for every row,
which rewrites (and WAL-logs) every page of `raw_data`.

## 2. What changed

Everything is in `services/parallel_expansion.py`, plus small hooks in
`services/postgres_service.py` and `services/psql_session.py`. The original engine
is kept and can be selected with `PDM_EXPAND_ENGINE=legacy`.

### 2.1 Phases from recipe dependencies
Destinations are grouped into commit phases. A phase ends when a recipe reads a
destination written earlier in the same phase, because other sessions can only see
those rows after they are committed. The official workload gives
`[table3, table4] → [table1] → [table5, table6, table2]`.

### 2.2 Range-parallel staging
A recipe is *row-local* when it is a plain `INSERT INTO t (cols) SELECT …` that reads
`{{source}}` once and has no `DISTINCT` / `GROUP BY` / window / `ORDER BY` / `LIMIT` /
`UNION` / `RETURNING` at the top level. Each row-local recipe is **staged**:
`K = nproc` server sessions each run the recipe's own SELECT over one contiguous
`raw_id` range, writing into an UNLOGGED staging table. The SQL recipe files are not
edited; at run time the engine makes only these value-preserving substitutions:

- the `{{source}}` relation, which the app already substitutes, gains a range
  predicate;
- one extra output column carries the source row key, so the original order can be
  rebuilt;
- **fields decoded once per row.** The recipes read every field as
  `src.raw -> 'values' ->> 'key'`; each call copies the whole `values` object before
  the key lookup, and a call inside a join condition runs once per candidate row
  (27 times per source row for `table6`). For a recipe that reads at least three
  fields, or reads one inside a join condition (`table1`, `table2`, `table6`), the
  range source decodes the referenced keys once per row with `jsonb_to_record`
  (`text` columns) and those calls become column references. `->>` and a `text`
  column of `jsonb_to_record` give the same text for every JSON type and NULL for a
  missing key; a non-object `values` gives NULL as before. `PDM_EXPAND_DECODE_FIELDS=off`
  disables it.

Recipes that are not row-local (`table3`, `table4`) run exactly as written. The manifest
scan, `table3` and `table4` read nothing from each other, so they run as **concurrent
transactions**.

### 2.3 Planner statistics between phases
A destination filled by an earlier phase has no statistics yet when the next phase's
recipes join it (autovacuum has not seen it). With no statistics the planner put the
4-row `table3` on the outer side of `table1`'s join and re-scanned each raw chunk once
per dimension row. Before a phase, the engine runs `ANALYZE` on each destination it
reads that was written earlier and has no large row estimate yet (`table3`, `table4`;
`table1`'s estimate is already set by its index build). Statistics only: rows and ids
are unchanged. `table1` staging at 1M: 5.1 s → 3.8 s.

### 2.4 One-transaction bulk load with constraints re-created before commit
After a phase is staged, one small transaction reserves **one contiguous block** of
each destination's identity sequence (`setval`) and records each range's first id.
Then **one transaction** per phase loads all ranges:

1. It locks each destination. If the destination is **empty** and owned by the current
   role, it saves the definition of every foreign key, `UNIQUE` constraint, secondary
   index and — when no foreign key points to it — the `PRIMARY KEY`
   (`pg_get_constraintdef` / `pg_get_indexdef`, plus any comment), then drops them.
   (`table1`'s primary key stays, because `table2/5/6` reference it.)
2. It appends every range in source order with explicit ids
   (`first_id + pgdm_ord − 1`, `OVERRIDING SYSTEM VALUE`) and updates each row counter
   in the same transaction as the rows it counts.
3. It re-creates every saved object from its own catalog definition — primary key,
   unique constraints, indexes (parallel index builds, `maintenance_work_mem=256MB`),
   then the foreign keys, which PostgreSQL re-validates over the whole table — and
   writes the journal row, then commits.

DDL is transactional in PostgreSQL: other sessions keep seeing the official schema the
whole time, and any failure (including a constraint violation found by the
re-validation) rolls the entire load back. The schema after the commit is identical to
the original (verified: every constraint, index, column, default, identity and
trigger). A non-empty destination, or one the role does not own, keeps every constraint
and gets row-by-row checks inside the same single transaction. `PDM_EXPAND_BULK_LOAD=off`
restores the earlier parallel row-by-row insert sessions.

Why it is faster: the row-by-row foreign-key triggers (7.3 million checks, including a
row lock on `raw_data` for every `table1` row) are replaced by one set-based validation
per foreign key, and the indexes are built once by sorting instead of being maintained
row by row. At 1M rows the two loads take 1.8 s and 4.7 s.

### 2.5 Identical output, including identity values
The original run assigns identity values in the order the single backend produces
rows: source order, and recipe order within a source row. Each staged row stores
`pgdm_ord`, its position in source order (nested-loop joins keep every source row's
output rows together and in recipe order), and the load gives it
`first_id + pgdm_ord − 1`.

`tools/challenge/verify_outputs.py` checks the result. It compares md5 digests of
every destination table ordered by primary key with all columns, plus the sequences,
`pgdm_table_row_counts`, and the `table_versions` / `table_version_dependencies`
rows. Result: **IDENTICAL** at 100k and 1M rows, both as superuser and as the
non-superuser owner role, with and without a vacuumed `raw_data`.

At 3M rows the original engine's own plan changes: its joins with `table1` become
multi-batch hash joins and it then assigns the child tables' ids in hash-batch order,
which depends on memory settings. This engine always assigns them in source order; on a
natural-key digest (surrogate ids replaced by the `raw_id` they refer to) the earlier
version of this engine was identical to the original at 3M.

### 2.6 Fewer round trips
- **One SSH channel** carries the whole data pipeline as a single bash program. The
  data never leaves the server.
- **Preflight checks** keep their exact SQL and error messages, but run in one
  persistent `psql` session per database instead of a new process each
  (`services/psql_session.py`). Preflight at 100k: 0.75 s → 0.07 s.

### 2.7 Honest timing
Every included item is still measured **on the server**: `clock_timestamp()` inside
each transaction and `date +%s%N` around each server-side block, whose wall time is the
included item. Overlapping per-session items are listed as excluded diagnostics, so
concurrent work is never hidden and never double-counted. Nothing that is part of the
expansion was moved outside the measured operation (driver wall time − MEASURED
PROCESSING TOTAL ≈ 0.8 s of Python start-up and SSH connect).

### 2.8 Single-core servers
Range-parallel staging is a second pass over the data that only pays off with more than
one core. `_use_parallel_expand_engine()` reads the server's `nproc` and falls back to the
original single-statement engine below 2 cores (an explicit `PDM_EXPAND_RANGES` still
forces the parallel path).

## 3. Failure recovery and consistency (journal + resume)
Two small tables are created in the data database: `public.pgdm_expansion_jobs` and
`public.pgdm_expansion_job_steps`.

- **Exactly-once steps.** Each step is journalled **in the same transaction** as the
  data it writes, so a step is either fully committed *and* journalled, or neither.
  The steps are: manifest, each inline recipe, each staging range, each id
  allocation, and each phase's bulk load.
- **Automatic resume.** Re-running the same expansion (same database, source,
  scopes and recipes, identified by a SHA-256 scope key) finds the unfinished job.
  It then:
  1. stops any sessions an interrupted run left behind (each session is tagged with
     `application_name = pgdm_expand:<job>`; an interrupted load transaction is rolled
     back, so its constraints are back in place);
  2. skips every completed step (staged ranges are kept, so only the interrupted step
     is redone);
  3. finishes the job.
  A different unfinished job blocks a new expansion with a clear message.
- **Server-side work survives a dropped connection.** The pipeline runs on the server;
  if the DB Manager or SSH dies, the server finishes the current steps, and the next
  Expand only registers what is left.
- **Crash safety.** Staging tables are UNLOGGED (no WAL) and are truncated if the
  server crashes, so a staging step only counts if it ran after the last postmaster
  start. Re-staging is deterministic: same rows, same ids.
- **Versions registered once.** If the run stopped after the data commit, the resumed
  run checks whether the versions were already registered and never registers them
  twice. Only then is the job marked `complete` and its staging tables dropped.
- **Tested at 1M rows as the non-superuser owner.** Two kinds of interruption, each
  followed by the same Expand again:
  - *client killed* (`docker kill` of the DB Manager process) during phase 1, during
    `table1` staging, and inside each load transaction — the server finished the
    started steps on its own and the next Expand completed the rest;
  - *client killed and every server session terminated* (`pg_terminate_backend`) inside
    the `table1` load, during `table5/6/2` staging, and inside the `table5/6/2` load — the
    load transaction rolled back, the schema was intact immediately, and the resume redid
    only the interrupted step (7.4 s for the interrupted `table5/6/2` load).

  Every resume produced an **IDENTICAL** database and an identical schema, with no
  staging tables or sessions left behind.

## 4. Resource efficiency (1M rows)
Measured the same way for all three (WAL = `pg_current_wal_lsn()` difference; temp =
`pg_stat_database.temp_bytes`; staging = size of the engine's UNLOGGED tables, sampled):

| 1M rows | original | earlier version (`b3df753`) | this solution |
|---|---:|---:|---:|
| WAL written | 2,935 MB | 3,297 MB | **2,469 MB** |
| Peak staging tables (UNLOGGED, dropped after each phase) | 0 | 255 MiB | 255 MiB (≈12 % of `raw_data`) |
| Temp files written (parallel index builds) | 0 | 0 | 210 MB |
| Peak container RAM (incl. 1 GB shared_buffers + page cache) | 2.2 GiB | 2.9 GiB | 2.8 GiB |
| Per-session `work_mem` | default | 64 MB × K | 64 MB × K; `maintenance_work_mem` 256 MB in the load |

At 100k rows: WAL 131 MB → 122 MB, peak staging 30 MiB, temp 13 MB.

Staging tables exist only for the phase that needs them and are dropped as soon as
their rows are loaded, so the peak is one phase's output, not the whole dataset. The
number of sessions is `nproc` of the server, capped at 16, and can be overridden with
`PDM_EXPAND_RANGES`.

## 5. Unchanged guarantees
- Same source and destination validation, function-safety checks and managed-DML
  row counters.
- Same version registration: one `cross_table_expand_v1` version per destination,
  original `sql_recipe`, dependency payload with the raw-hash manifest, and no dump.
- `generate_database.py`, the seed, the SQL recipe files and the schema are untouched.
  Every constraint and index is present and validated after each commit (they are
  re-created inside the load transaction). Nothing is hard-coded or precomputed.
- The GUI (`main.py`) is unchanged: it calls the same
  `execute_cross_table_expansion` / `register_cross_table_expansion_versions`.

## 6. Reproduce
1. Use the original setup from `README.md` / `GENERATE_DATABASE_README.md`
   (PostgreSQL ≥ 13 on Linux with `bash`, `psql`, `nproc`; Python ≥ 3.10 with
   `requirements.txt`).
2. Generate a benchmark database with the official generator.
3. Open the DB Manager, connect over SSH, and run **Expand** exactly as before. The
   parallel engine is the default.
4. To compare, run once with `PDM_EXPAND_ENGINE=legacy` on a freshly generated
   database. To test recovery, interrupt the run (close the app or cut the network),
   then run the same Expand again: it resumes.
5. Scripted alternative to the GUI: [`tools/challenge/`](tools/challenge/README.md).
   - `run_expand.py` runs the headless Expand and prints the same report.
   - `verify_outputs.py` writes or compares the exact-output digest.
   - `crash_resume_test.py` kills an Expand after N seconds, resumes it, and verifies
     the result.

Configuration (environment variables): `PDM_EXPAND_ENGINE` = `parallel` (default) or
`legacy`; `PDM_EXPAND_RANGES` = `auto` (default, server `nproc`) or a number;
`PDM_EXPAND_BULK_LOAD` = `on` (default) or `off`; `PDM_EXPAND_DECODE_FIELDS` = `on`
(default) or `off`.

Tests: `python -m pytest tests -q`. These include unit tests in
`tests/test_parallel_expansion.py`. The legacy timing test is pinned to the legacy
engine.

## 7. Known limitations and trade-offs
- During an expansion the destinations are written by several transactions (one per
  phase step) instead of one. A crash leaves a consistent, journalled partial state that
  the next Expand completes, instead of "all-or-nothing, restart from zero".
- Each load transaction holds `ACCESS EXCLUSIVE` locks on its destinations and on the
  tables their foreign keys point to (`raw_data`, `table3`, `table4`, `table1`) until it
  commits — about 2 s and 5 s at 1M rows. Other sessions wait; they never see the
  dropped constraints.
- The load of a phase is one transaction: if it is interrupted, that load is redone from
  the staged rows (staging is kept). At very large sizes the index builds may use
  temporary files (`maintenance_work_mem=256MB`).
- Recipes that are not row-local fall back to the original single-statement path. This
  is correct but not parallel.
- Exact identity equality relies on the source being read in `raw_id` order, which is
  how the official generator lays out the data. A different physical order would give a
  different but equally valid id assignment.
- Not tested on Windows here. The changes are server-side and in the service layer, and
  the GUI code is unchanged.
