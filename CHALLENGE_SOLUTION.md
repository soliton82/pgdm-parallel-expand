# Parallel, resumable Expand — solution notes

Kaggle: *Python & PostgreSQL Data Pipeline Optimization Challenge*. Author: soliton82.
Licence: Apache-2.0 (same as the repository).

**In short:** the Expand operation now spreads the work of each recipe across all
CPU cores of the PostgreSQL server. Every step commits with a journal entry, so an
interrupted run resumes where it stopped. The six official recipes run **unchanged**.
The final database is **byte-identical** to the original engine's result, including
every generated identity value, the sequence states, the row counters, and the
version/dependency records.

| Local benchmark (MEASURED PROCESSING TOTAL) | original engine | this solution | speed-up |
|---|---:|---:|---:|
| 100,000 `raw_data` rows | 8.76 s | **2.81 s** | 3.1× |
| 1,000,000 `raw_data` rows | 84.6 – 102.9 s | **28.8 – 31.7 s** | 2.9 – 3.3× |

*These are local results, not official scores.* Hardware: Apple M3 Pro. The Linux
server is an aarch64 VM with 6 vCPUs and 8 GB RAM, running PostgreSQL 16 (Docker
`postgres:16`). The DB Manager talks to it over SSH and `psql`, exactly like the
original architecture. Databases come from the official, unmodified
`generate_database.py`. Each figure is the minimum of the repeated runs; the ranges
show run-to-run spread.

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
the `SELECT` of an `INSERT … SELECT`, so the server's other cores sit idle, and the
cost grows linearly with the number of rows.

## 2. What changed

Everything is in `services/parallel_expansion.py`, plus small hooks in
`services/postgres_service.py` and `services/psql_session.py`. The original engine
is kept and can be selected with `PDM_EXPAND_ENGINE=legacy`.

### 2.1 Phases from recipe dependencies
Destinations are grouped into commit phases. A phase ends when a recipe reads a
destination written earlier in the same phase, because other sessions can only see
those rows after they are committed. The official workload gives
`[table3, table4] → [table1] → [table5, table6, table2]`.

### 2.2 Range-parallel staging (the speed-up)
A recipe is *row-local* when it is a plain `INSERT INTO t (cols) SELECT …` that reads
`{{source}}` once and has no `DISTINCT` / `GROUP BY` / window / `ORDER BY` / `LIMIT` /
`UNION` / `RETURNING` at the top level. Each row-local recipe is **staged**:
`K = nproc` server sessions each run the recipe's own SELECT over one contiguous
`raw_id` range, writing into an UNLOGGED staging table. The recipe text is not
edited. The only changes are:

- the `{{source}}` relation, which the app already substitutes, gains a range
  predicate;
- one extra output column carries the source row key, so the original order can be
  rebuilt.

Recipes that are not row-local (`table3`, `table4`) run as written. The manifest scan,
`table3` and `table4` read nothing from each other, so they run as **concurrent
transactions**.

### 2.3 Identical output, including identity values
The original run assigns identity values in the order the single backend produces
rows: source order, and recipe order within a source row. The new engine reproduces
this exactly:

1. Each staged row stores `pgdm_ord`, its position in source order. Nested-loop
   joins keep every source row's output rows together and in recipe order.
2. One small transaction reads the staged row counts and reserves **one contiguous
   block** of the destination's identity sequence (`setval`). It records each
   range's first id in a journal table.
3. `K` sessions insert their ranges **in parallel** with explicit ids
   (`first_id + pgdm_ord − 1`, `OVERRIDING SYSTEM VALUE`). Every PK, unique index and
   foreign key is enforced by PostgreSQL as before. The row counter is updated in
   the same transaction as the rows it counts.

`src/verify_outputs.py` (in the benchmark harness, see §6) checks the result. It
compares md5 digests of every destination table ordered by primary key with all
columns, plus the sequences, `pgdm_table_row_counts`, and the
`table_versions` / `table_version_dependencies` rows. Result: **IDENTICAL** at 100k
and 1M rows.

### 2.4 Fewer round trips
- **One SSH channel** carries the whole data pipeline as a single bash program. The
  data never leaves the server.
- **Preflight checks** keep their exact SQL and error messages, but run in one
  persistent `psql` session per database instead of a new process each
  (`services/psql_session.py`). The session keeps `ON_ERROR_STOP=1` semantics.
  Preflight at 100k: 0.75 s → 0.07 s.

### 2.5 Honest timing
Every included item is still measured **on the server**:

- `clock_timestamp()` inside each phase transaction;
- `date +%s%N` around each parallel block. The block's wall time is the included
  item. Overlapping per-session items are listed as excluded diagnostics, so
  concurrent work is never hidden and never double-counted.

Nothing that is part of the expansion was moved outside the measured operation.

## 3. Failure recovery and consistency (journal + resume)
Two small tables are created in the data database: `public.pgdm_expansion_jobs` and
`public.pgdm_expansion_job_steps`.

- **Exactly-once steps.** Each step is journalled **in the same transaction** as the
  data it writes, so a step is either fully committed *and* journalled, or neither.
  The steps are: manifest, each inline recipe, each staging range, each id
  allocation, and each range insert (via a `done` flag on its allocation row).
- **Automatic resume.** Re-running the same expansion (same database, source,
  scopes and recipes, identified by a SHA-256 scope key) finds the unfinished job.
  It then:
  1. stops any sessions an interrupted run left behind (each session is tagged with
     `application_name = pgdm_expand:<job>`);
  2. skips every completed step;
  3. finishes the job.
  A different unfinished job blocks a new expansion with a clear message.
- **Crash safety.** Staging tables are UNLOGGED (no WAL) and are truncated if the
  server crashes, so a staging step only counts if it ran after the last postmaster
  start. Re-staging is deterministic: same rows, same ids. The allocation journal is
  a normal logged table.
- **Versions registered once.** If the run stopped after the data commit, the
  resumed run checks whether the versions were already registered and never
  registers them twice. Only then is the job marked `complete` and its staging
  tables dropped.
- **Tested:** `kill -9` of the DB Manager process during the `table1` staging, and
  again after the `table5/6/2` staging, at 1M rows. Both resumes produced an
  IDENTICAL database (digest above).

## 4. Resource efficiency (1M rows)
| | original | this solution |
|---|---:|---:|
| Peak container RAM (incl. 1 GB shared_buffers + page cache) | 3.35 GiB | 3.54 GiB |
| Peak staging tables (temporary, UNLOGGED) | 0 | 255 MiB (≈12% of `raw_data`) |
| Temp files (`pg_ls_tmpdir`) | 0 | 0 |
| Per-session `work_mem` | default | 64 MB × K sessions |

Staging tables exist only for the phase that needs them. They are dropped as soon
as their rows are inserted, so the peak is one phase's output, not the whole
dataset. The number of sessions is `nproc` of the server, capped at 16, and can be
overridden with `PDM_EXPAND_RANGES`.

## 5. Unchanged guarantees
- Same source and destination validation, function-safety checks and managed-DML
  row counters.
- Same version registration: one `cross_table_expand_v1` version per destination,
  original `sql_recipe`, dependency payload with the raw-hash manifest, and no dump.
- `generate_database.py`, the seed, the schema, the SQL recipe files and every
  constraint and index are untouched. Nothing is hard-coded or precomputed.
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

Configuration (environment variables): `PDM_EXPAND_ENGINE` = `parallel` (default) or
`legacy`; `PDM_EXPAND_RANGES` = `auto` (default, server `nproc`) or a number.

Tests: `python -m pytest tests -q`. These include new unit tests in
`tests/test_parallel_expansion.py`. The legacy timing test is pinned to the legacy
engine.

## 7. Known limitations and trade-offs
- During an expansion the destinations are written by several transactions, one per
  phase or range, instead of one. A crash leaves a consistent, journalled partial
  state that the next Expand completes. This replaces "all-or-nothing, restart from
  zero".
- Recipes that are not row-local fall back to the original single-statement path.
  This is correct but not parallel.
- Exact identity equality relies on the source being read in `raw_id` order, which
  is how the official generator lays out the data. A different physical order would
  give a different but equally valid id assignment.
- Not tested on Windows here. The changes are server-side and in the service layer,
  and the GUI code is unchanged.
