"""Parallel, phase-committed engine for the cross-table Expand operation.

The legacy engine streams one transaction that runs every recipe as a single
``INSERT ... SELECT`` in one backend. PostgreSQL never parallelises the SELECT
of an ``INSERT ... SELECT``, so a 300M-row expansion is bound to one CPU core.

This engine keeps the recipes untouched and changes only *where* their SELECT
runs:

1. Destinations are grouped into **phases**. A phase ends when a recipe reads a
   destination written earlier in the same phase (its rows must be committed
   before other sessions can see them). For the official workload this gives
   ``[table3, table4] -> [table1] -> [table5, table6, table2]``.
2. Inside a phase, every *row-local* recipe (a plain ``INSERT INTO t (cols)
   SELECT ...`` that reads the source exactly once and has no DISTINCT /
   GROUP BY / window / ORDER BY / LIMIT at the top level) is **staged**: K
   server-side sessions each run the recipe's SELECT over one contiguous
   ``raw_id`` range into an UNLOGGED staging table. Other recipes run inline.
3. One transaction per phase then inserts the staged rows into the real
   destination **in range order** (``UNION ALL`` of the ranges), so identity
   values come out in the same order as the single-pass legacy run, and every
   constraint, index and foreign key is enforced exactly as before. Row counters
   and telemetry are updated through the same managed-DML wrapper as the legacy
   engine, inside the same transaction as the rows they count.

The whole pipeline is shipped as ONE bash script over ONE SSH channel; the
PostgreSQL work never leaves the server. Staging wall time is measured on the
server with ``date +%s%N`` and reported as included telemetry, so the
MEASURED PROCESSING TOTAL keeps covering every step of the expansion.
"""
from __future__ import annotations

import json
import re
import secrets
import shlex

from utils import sql_ident, sql_literal

STAGE_PREFIX = "pgdm_xstage_"
_INSERT_RE = re.compile(
    r"^\s*INSERT\s+INTO\s+(?P<dest>[^\s(]+)\s*\((?P<cols>[^()]*)\)\s*(?P<select>SELECT\b.*)$",
    re.IGNORECASE | re.DOTALL,
)
_NOT_ROW_LOCAL_RE = re.compile(
    r"\b(SELECT\s+DISTINCT|GROUP\s+BY|ORDER\s+BY|LIMIT|OFFSET|OVER\s*\(|WINDOW|RETURNING|ON\s+CONFLICT|WITH\s+RECURSIVE|UNION|INTERSECT|EXCEPT)\b",
    re.IGNORECASE,
)


def source_relation_sql(source_schema_name, source_table_name, raw_schemas) -> str:
    """The exact relation text the legacy preparation substitutes for {{source}}."""
    scope_sql = ", ".join(sql_literal(value) for value in raw_schemas)
    return (
        "(\n"
        f"    SELECT *\n"
        f"    FROM {sql_ident(source_schema_name)}.{sql_ident(source_table_name)}\n"
        f"    WHERE {sql_ident('raw_schema')} IN ({scope_sql})\n"
        ")"
    )


def _range_source_sql(source_schema_name, source_table_name, raw_schemas, k, ranges) -> str:
    """Source restricted to the k-th of `ranges` contiguous raw_id slices."""
    table = f"{sql_ident(source_schema_name)}.{sql_ident(source_table_name)}"
    scope = f"{sql_ident('raw_schema')} IN ({', '.join(sql_literal(v) for v in raw_schemas)})"
    rid = sql_ident("raw_id")

    def bound(i):
        return (f"(SELECT min({rid}) + (max({rid}) - min({rid}) + 1) * {int(i)} / {int(ranges)} "
                f"FROM {table} WHERE {scope})")

    conditions = [scope]
    if k > 0:
        conditions.append(f"{rid} >= {bound(k)}")
    if k < ranges - 1:
        conditions.append(f"{rid} < {bound(k + 1)}")
    return f"(\n    SELECT *\n    FROM {table}\n    WHERE {' AND '.join(conditions)}\n)"


def split_statement(statement: str, source_sql: str):
    """Return (destination, [columns], select_sql) for a stageable statement, else None."""
    if statement.count(source_sql) != 1:
        return None
    match = _INSERT_RE.match(statement.strip().rstrip(";"))
    if not match:
        return None
    select_sql = match.group("select")
    # Inspect the recipe text outside the substituted source relation only.
    if _NOT_ROW_LOCAL_RE.search(select_sql.replace(source_sql, "")):
        return None
    columns = [c.strip() for c in match.group("cols").split(",") if c.strip()]
    alias = re.search(re.escape(source_sql) + r"\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)", select_sql, re.IGNORECASE)
    if not columns or not alias or alias.group(1).upper() in {"JOIN", "WHERE", "CROSS", "LEFT", "INNER", "ON"}:
        return None
    # Carry the source row key so the staged rows can be put back into source order.
    keyed = re.sub(r"^SELECT\b", f"SELECT {alias.group(1)}.raw_id AS pgdm_order_key,", select_sql, count=1, flags=re.IGNORECASE)
    return match.group("dest"), columns, keyed


def _relation_key(name: str) -> str:
    parts = [p.strip().strip('"').lower() for p in str(name).split(".")]
    return ".".join(parts if len(parts) == 2 else ["public"] + parts)


def plan_phases(destinations, source_sql: str):
    """Group prepared destinations into commit phases (see module docstring)."""
    phases, written = [], set()
    for destination in destinations:
        items = []
        for index, statement in enumerate(destination["statements"], start=1):
            items.append({
                "destination": destination,
                "statement": statement,
                "statement_index": index,
                "split": split_statement(statement, source_sql),
            })
        reads = {_relation_key(f"{s}.{t}") for s, t in destination.get("read_relations") or []}
        staged = any(item["split"] for item in items)
        if not phases or (staged and reads & written):
            phases.append([])
            written = set()
        phases[-1].extend(items)
        written.add(_relation_key(destination["table_name"]))
    return phases


def build_pipeline_script(service, *, database_name, source_schema_name, source_table_name,
                          raw_schemas, prepared_destinations, ranges, timing_table_sql,
                          timing_result_sql, manifest_sql):
    """Return (bash_script, stage_tables) implementing the whole expansion."""
    source_sql = source_relation_sql(source_schema_name, source_table_name, raw_schemas)
    phases = plan_phases(prepared_destinations, source_sql)
    tag = secrets.token_hex(4)
    psql = (f"psql -h localhost -U {shlex.quote(service.sql_username)} -d {shlex.quote(database_name)} "
            "-X -qAt -v ON_ERROR_STOP=1")
    lines = [
        "set -u",
        f"PGDM_PSQL={shlex.quote(psql)}",
        "pgdm_cleanup() { $PGDM_PSQL >/dev/null 2>&1 <<'PGDM_CLEANUP_SQL'",
        "SET client_min_messages = warning;",
        "DO $pgdm_cleanup$ DECLARE r record; BEGIN",
        f"  FOR r IN SELECT schemaname, tablename FROM pg_tables WHERE tablename LIKE '{STAGE_PREFIX}{tag}%' LOOP",
        "    EXECUTE format('DROP TABLE IF EXISTS %I.%I', r.schemaname, r.tablename);",
        "  END LOOP; END $pgdm_cleanup$;",
        "PGDM_CLEANUP_SQL",
        "}",
        "trap pgdm_cleanup EXIT",
    ]
    stage_tables = []
    sequence = 10
    for phase_number, phase in enumerate(phases, start=1):
        staged = [item for item in phase if item["split"]]
        # --- parallel staging: one session per raw_id range, all staged recipes of the phase
        if staged:
            for item in staged:
                item["stage_tables"] = [
                    f"public.{STAGE_PREFIX}{tag}_{item['destination']['pure_table_name']}_{item['statement_index']}_{k}"
                    for k in range(ranges)
                ]
                stage_tables.extend(item["stage_tables"])
            lines.append("pgdm_ts=$(date +%s%N); pgdm_pids=''")
            for k in range(ranges):
                # Nested loops keep every source row's output rows consecutive and in
                # recipe order; row_number() records that order before the sort by key.
                sql = [
                    "SET client_min_messages = warning;",
                    "SET max_parallel_workers_per_gather = 0;",
                    "SET enable_hashjoin = off;",
                    "SET enable_mergejoin = off;",
                    "SET work_mem = '64MB';",
                ]
                for item in staged:
                    _, columns, select_sql = item["split"]
                    alias = ", ".join(f"pgdm_c{i}" for i in range(len(columns)))
                    plain = ", ".join(f"pgdm_q.pgdm_c{i}" for i in range(len(columns)))
                    select_k = select_sql.replace(
                        source_sql,
                        _range_source_sql(source_schema_name, source_table_name, raw_schemas, k, ranges),
                    )
                    stage = item["stage_tables"][k]
                    sql.append(f"DROP TABLE IF EXISTS {stage};")
                    sql.append(
                        f"CREATE UNLOGGED TABLE {stage} AS SELECT {plain} FROM (\n"
                        f"SELECT pgdm_r.*, row_number() OVER () AS pgdm_seq FROM (\n{select_k}\n) AS pgdm_r\n"
                        f") AS pgdm_q(pgdm_order_key, {alias}, pgdm_seq) ORDER BY pgdm_q.pgdm_order_key, pgdm_q.pgdm_seq;")
                delimiter = f"PGDM_STAGE_{phase_number}_{k}_SQL"
                lines.append(f"$PGDM_PSQL >/dev/null <<'{delimiter}' &")
                lines.extend(sql)
                lines.append(delimiter)
                lines.append('pgdm_pids="$pgdm_pids $!"')
            lines.append("pgdm_fail=0; for p in $pgdm_pids; do wait $p || pgdm_fail=1; done")
            lines.append("pgdm_te=$(date +%s%N)")
            names = ", ".join(item["destination"]["table_name"] for item in staged)
            label = f"Transform source in {ranges} parallel raw_id ranges for {names}"
            lines.append(
                "printf 'PGDM_STAGE_TIMING %s %s\\n' \"$((pgdm_te - pgdm_ts))\" "
                + shlex.quote(json.dumps({"sequence": sequence, "name": label}))
            )
            sequence += 1
            lines.append('[ "$pgdm_fail" -eq 0 ] || { echo "PGDM_STAGE_FAILED phase ' + str(phase_number) + '" >&2; exit 4; }')

        # --- one transaction per phase: inline recipes + ordered inserts from the stages
        body = [timing_table_sql, "BEGIN ISOLATION LEVEL REPEATABLE READ;",
                "SET LOCAL client_min_messages = warning;",
                "SET LOCAL max_parallel_workers_per_gather = 0;"]
        lock_sql = service._build_cross_table_expansion_lock_sql(
            list({id(item["destination"]): item["destination"] for item in phase}.values()))
        body += [f"INSERT INTO pg_temp.pgdm_expansion_timings VALUES ({sequence}, 'execute', "
                 f"{sql_literal(f'Acquire destination table locks (phase {phase_number})')}, clock_timestamp(), NULL, NULL, NULL, NULL);",
                 lock_sql,
                 f"UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = {sequence};"]
        sequence += 1
        if phase_number == 1:
            body += [f"INSERT INTO pg_temp.pgdm_expansion_timings VALUES ({sequence}, 'execute', "
                     "'Scan source and build dependency manifest', clock_timestamp(), NULL, NULL, NULL, NULL);",
                     manifest_sql,
                     f"UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = {sequence};"]
            sequence += 1
        for item in phase:
            destination = item["destination"]
            if item["split"]:
                dest, columns, _ = item["split"]
                union = "\nUNION ALL\n".join(f"SELECT * FROM {t}" for t in item["stage_tables"])
                statement = f"INSERT INTO {dest} (\n    {', '.join(columns)}\n)\n{union}"
            else:
                statement = item["statement"]
            body.append(service._build_managed_dml_sql(
                statement, destination["schema_name"], destination["pure_table_name"], "add",
                timing_sequence=sequence, timing_stage="execute",
                timing_name=f"Insert into {destination['table_name']} (statement {item['statement_index']})",
                timing_destination=destination["table_name"],
                timing_statement_index=item["statement_index"],
            ))
            sequence += 1
            for table in item.get("stage_tables") or []:
                body.append(f"DROP TABLE {table};")
        body += [f"INSERT INTO pg_temp.pgdm_expansion_timings VALUES ({sequence}, 'execute', "
                 f"{sql_literal(f'Commit phase {phase_number}')}, clock_timestamp(), NULL, NULL, NULL, NULL);",
                 "COMMIT;",
                 f"UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = {sequence};",
                 timing_result_sql]
        sequence += 1
        delimiter = f"PGDM_PHASE_{phase_number}_SQL"
        lines.append(f"$PGDM_PSQL <<'{delimiter}' || exit 5")
        lines.extend(body)
        lines.append(delimiter)
    return "\n".join(lines) + "\n", phases, stage_tables


def parse_pipeline_output(output: str):
    """Collect the manifest, all phase timing items and the staging timings."""
    manifest, items = None, []
    for line in output.splitlines():
        candidate = line.strip()
        if candidate.startswith("PGDM_STAGE_TIMING "):
            _, ns, payload = candidate.split(" ", 2)
            meta = json.loads(payload)
            items.append({"sequence": meta["sequence"], "stage": "execute", "name": meta["name"],
                          "duration_seconds": int(ns) / 1e9, "rows": None, "destination": None})
            continue
        if not candidate.startswith("{"):
            continue
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and "raw_hash_manifest" in parsed:
            manifest = parsed
        if isinstance(parsed, dict) and "pgdm_expansion_timings" in parsed:
            items.extend(parsed.get("pgdm_expansion_timings") or [])
    items.sort(key=lambda item: item.get("sequence") or 0)
    return manifest, items
