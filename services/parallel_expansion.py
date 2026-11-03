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
    alloc = f"public.{STAGE_PREFIX}{tag}_alloc"
    lines += [
        "$PGDM_PSQL >/dev/null <<'PGDM_ALLOC_SQL' || exit 3",
        "SET client_min_messages = warning;",
        f"CREATE UNLOGGED TABLE {alloc} (destination text, statement_index integer, range_index integer, "
        "rows bigint, first_id bigint, id_increment bigint, id_column text, done boolean NOT NULL DEFAULT false, "
        "PRIMARY KEY (destination, statement_index, range_index));",
        "PGDM_ALLOC_SQL",
    ]
    state = {"sequence": 10}

    def next_sequence():
        state["sequence"] += 1
        return state["sequence"]

    def timed_block(label, body_lines):
        """Wrap shell lines with a server-side wall-clock measurement (included telemetry)."""
        seq = next_sequence()
        lines.append("pgdm_ts=$(date +%s%N)")
        lines.extend(body_lines)
        lines.append("pgdm_te=$(date +%s%N)")
        lines.append("printf 'PGDM_STAGE_TIMING %s %s\\n' \"$((pgdm_te - pgdm_ts))\" "
                     + shlex.quote(json.dumps({"sequence": seq, "name": label})))

    def parallel(scripts, failure_code, what):
        body = ["pgdm_pids=''"]
        for k, sql in enumerate(scripts):
            delimiter = f"PGDM_{what}_{state['sequence']}_{k}_SQL"
            body += [f"$PGDM_PSQL >/dev/null <<'{delimiter}' &", *sql, delimiter, 'pgdm_pids="$pgdm_pids $!"']
        body += ["pgdm_fail=0; for p in $pgdm_pids; do wait $p || pgdm_fail=1; done"]
        return body, f'[ "$pgdm_fail" -eq 0 ] || {{ echo "PGDM_{what}_FAILED" >&2; exit {failure_code}; }}'

    def inline_transaction(items, phase_number, with_manifest):
        body = [timing_table_sql, "BEGIN ISOLATION LEVEL REPEATABLE READ;",
                "SET LOCAL client_min_messages = warning;"]
        if items:
            seq = next_sequence()
            body += [f"INSERT INTO pg_temp.pgdm_expansion_timings VALUES ({seq}, 'execute', "
                     f"{sql_literal(f'Acquire destination table locks (phase {phase_number})')}, clock_timestamp(), NULL, NULL, NULL, NULL);",
                     service._build_cross_table_expansion_lock_sql(
                         list({id(i["destination"]): i["destination"] for i in items}.values())),
                     f"UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = {seq};"]
        if with_manifest:
            seq = next_sequence()
            body += [f"INSERT INTO pg_temp.pgdm_expansion_timings VALUES ({seq}, 'execute', "
                     "'Scan source and build dependency manifest', clock_timestamp(), NULL, NULL, NULL, NULL);",
                     manifest_sql,
                     f"UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = {seq};"]
        for item in items:
            destination = item["destination"]
            body.append(service._build_managed_dml_sql(
                item["statement"], destination["schema_name"], destination["pure_table_name"], "add",
                timing_sequence=next_sequence(), timing_stage="execute",
                timing_name=f"Insert into {destination['table_name']} (statement {item['statement_index']})",
                timing_destination=destination["table_name"],
                timing_statement_index=item["statement_index"],
            ))
        seq = next_sequence()
        body += [f"INSERT INTO pg_temp.pgdm_expansion_timings VALUES ({seq}, 'execute', "
                 f"{sql_literal(f'Commit phase {phase_number}')}, clock_timestamp(), NULL, NULL, NULL, NULL);",
                 "COMMIT;",
                 f"UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = {seq};",
                 timing_result_sql]
        delimiter = f"PGDM_TXN_{seq}_SQL"
        lines.extend([f"$PGDM_PSQL <<'{delimiter}' || exit 5", *body, delimiter])

    def allocation_sql(item):
        dest, columns, _ = item["split"]
        excluded = ", ".join(sql_literal(c.strip('"').lower()) for c in columns)
        stmt = item["statement_index"]
        counts = "\n".join(
            f"  EXECUTE 'SELECT count(*) FROM {t}' INTO v_n;\n"
            f"  INSERT INTO {alloc} VALUES ({sql_literal(dest)}, {stmt}, {k}, v_n, "
            f"CASE WHEN v_seq IS NULL THEN NULL ELSE v_start + v_total * v_inc END, v_inc, v_col);\n"
            f"  v_total := v_total + v_n;"
            for k, t in enumerate(item["stage_tables"]))
        return f"""DO $pgdm_alloc$
DECLARE v_col text; v_seq text; v_last bigint; v_called boolean; v_inc bigint := 1; v_start bigint; v_total bigint := 0; v_n bigint;
BEGIN
  -- Reserve one contiguous block of the destination's identity/serial values, in source order.
  SELECT a.attname, pg_get_serial_sequence({sql_literal(dest)}, a.attname) INTO v_col, v_seq
  FROM pg_attribute a
  WHERE a.attrelid = {sql_literal(dest)}::regclass AND a.attnum > 0 AND NOT a.attisdropped
    AND pg_get_serial_sequence({sql_literal(dest)}, a.attname) IS NOT NULL
    AND lower(a.attname) <> ALL (ARRAY[{excluded}]::text[])
  ORDER BY a.attnum LIMIT 1;
  IF v_seq IS NOT NULL THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('pgdm_expand_alloc:' || v_seq, 0));
    EXECUTE format('SELECT last_value, is_called FROM %s', v_seq) INTO v_last, v_called;
    SELECT seqincrement INTO v_inc FROM pg_sequence WHERE seqrelid = v_seq::regclass;
    v_start := CASE WHEN v_called THEN v_last + v_inc ELSE v_last END;
  END IF;
{counts}
  IF v_seq IS NOT NULL AND v_total > 0 THEN
    PERFORM setval(v_seq, v_start + (v_total - 1) * v_inc, true);
  END IF;
END $pgdm_alloc$;"""

    def insert_sql(item, k):
        destination = item["destination"]
        dest, columns, _ = item["split"]
        stage = item["stage_tables"][k]
        values = ", ".join(f"pgdm_c{i}" for i in range(len(columns)))
        col_list = ", ".join(columns)
        stmt = item["statement_index"]
        return f"""BEGIN;
SET LOCAL client_min_messages = warning;
SET LOCAL max_parallel_workers_per_gather = 0;
DO $pgdm_insert$
DECLARE r record; v_rows bigint;
BEGIN
  SELECT * INTO r FROM {alloc} WHERE destination = {sql_literal(dest)} AND statement_index = {stmt} AND range_index = {k} FOR UPDATE;
  IF r.done THEN RETURN; END IF;
  IF r.id_column IS NULL THEN
    INSERT INTO {dest} ({col_list}) SELECT {values} FROM {stage} ORDER BY pgdm_ord;
  ELSE
    EXECUTE format('INSERT INTO {dest} (%I, {col_list.replace('%', '%%')}) OVERRIDING SYSTEM VALUE '
                   'SELECT $1 + (pgdm_ord - 1) * $2, {values} FROM {stage}', r.id_column)
      USING r.first_id, r.id_increment;
  END IF;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  UPDATE {alloc} SET done = true WHERE destination = {sql_literal(dest)} AND statement_index = {stmt} AND range_index = {k};
  UPDATE {service_counter_table} SET row_count = row_count + v_rows, updated_at = clock_timestamp()
  WHERE schema_name = {sql_literal(destination['schema_name'])} AND table_name = {sql_literal(destination['pure_table_name'])};
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Contador ausente para %.% durante insercao.', {sql_literal(destination['schema_name'])}, {sql_literal(destination['pure_table_name'])};
  END IF;
END $pgdm_insert$;
COMMIT;"""

    from services.postgres_service import DATA_TABLE_ROW_COUNTS as service_counter_table

    for phase_number, phase in enumerate(phases, start=1):
        staged = [item for item in phase if item["split"]]
        # --- 1. parallel staging: one session per raw_id range, every staged recipe of the phase
        if staged:
            for item in staged:
                item["stage_tables"] = [
                    f"public.{STAGE_PREFIX}{tag}_{item['destination']['pure_table_name']}_{item['statement_index']}_{k}"
                    for k in range(ranges)
                ]
                stage_tables.extend(item["stage_tables"])
            scripts = []
            for k in range(ranges):
                # Nested loops keep each source row's output rows consecutive and in recipe
                # order; pgdm_seq records that order, pgdm_ord is the final source order.
                sql = ["SET client_min_messages = warning;",
                       "SET max_parallel_workers_per_gather = 0;",
                       "SET enable_hashjoin = off;", "SET enable_mergejoin = off;",
                       "SET work_mem = '64MB';"]
                for item in staged:
                    _, columns, select_sql = item["split"]
                    alias = ", ".join(f"pgdm_c{i}" for i in range(len(columns)))
                    plain = ", ".join(f"pgdm_q.pgdm_c{i}" for i in range(len(columns)))
                    select_k = select_sql.replace(
                        source_sql, _range_source_sql(source_schema_name, source_table_name, raw_schemas, k, ranges))
                    stage = item["stage_tables"][k]
                    sql += [f"DROP TABLE IF EXISTS {stage};",
                            f"CREATE UNLOGGED TABLE {stage} AS SELECT row_number() OVER (ORDER BY pgdm_q.pgdm_order_key, pgdm_q.pgdm_seq) AS pgdm_ord, {plain} FROM (\n"
                            f"SELECT pgdm_r.*, row_number() OVER () AS pgdm_seq FROM (\n{select_k}\n) AS pgdm_r\n"
                            f") AS pgdm_q(pgdm_order_key, {alias}, pgdm_seq);"]
                scripts.append(sql)
            names = ", ".join(item["destination"]["table_name"] for item in staged)
            body, check = parallel(scripts, 4, "STAGE")
            timed_block(f"Transform source in {ranges} parallel raw_id ranges for {names}", body)
            lines.append(check)

        # --- 2. walk the phase in recipe order: inline runs go through one transaction,
        #        staged runs are inserted by `ranges` parallel sessions with pre-allocated ids.
        groups = []
        for item in phase:
            kind = "staged" if item["split"] else "inline"
            if not groups or groups[-1][0] != kind:
                groups.append((kind, []))
            groups[-1][1].append(item)
        for group_index, (kind, items) in enumerate(groups):
            if kind == "inline":
                first = phase_number == 1 and group_index == 0
                written_here = {_relation_key(i["destination"]["table_name"]) for i in items}
                independent = len(items) > 1 and all(
                    not ({_relation_key(f"{a}.{b}") for a, b in i["destination"].get("read_relations") or []} & written_here)
                    for i in items)
                if not independent:
                    inline_transaction(items, phase_number, with_manifest=first)
                    continue
                # Recipes that do not read each other's output run as concurrent
                # transactions (same statements and plans as the legacy engine).
                saved, lines[:] = list(lines), []
                inline_transaction([], phase_number, with_manifest=True) if first else None
                for item in items:
                    inline_transaction([item], phase_number, with_manifest=False)
                txns, lines[:] = list(lines), saved
                body, i = ["pgdm_pids=''", "pgdm_out=$(mktemp -d)"], 0
                for line in txns:
                    if line.startswith("$PGDM_PSQL <<'PGDM_TXN_") and line.endswith("|| exit 5"):
                        body.append(line.replace(" || exit 5", f" > $pgdm_out/{i}.out &"))
                        i += 1
                    elif line.startswith("PGDM_TXN_") and line.endswith("_SQL"):
                        body += [line, 'pgdm_pids="$pgdm_pids $!"']
                    else:
                        body.append(line)
                body += ["pgdm_fail=0; for p in $pgdm_pids; do wait $p || pgdm_fail=1; done"]
                # The block's wall time is the included measurement; the per-session
                # items inside it overlap in time and are reported as diagnostics only.
                timed_block("Run " + ", ".join(["source manifest"] if first else [])
                            + ("" if not first else ", ") + ", ".join(i["destination"]["table_name"] for i in items)
                            + f" as {len(items) + int(first)} concurrent transactions", body)
                lines += ["echo PGDM_CONCURRENT_BEGIN", "cat $pgdm_out/*.out; rm -rf $pgdm_out", "echo PGDM_CONCURRENT_END"]
                lines.append('[ "$pgdm_fail" -eq 0 ] || { echo "PGDM_INLINE_FAILED" >&2; exit 5; }')
                continue
            if phase_number == 1 and group_index == 0:
                inline_transaction([], phase_number, with_manifest=True)
            names = ", ".join(item["destination"]["table_name"] for item in items)
            alloc_body = ["$PGDM_PSQL >/dev/null <<'PGDM_ALLOCATE_" + str(state["sequence"]) + "_SQL' || exit 6",
                          "SET client_min_messages = warning;", "BEGIN;",
                          *[allocation_sql(item) for item in items], "COMMIT;",
                          "PGDM_ALLOCATE_" + str(state["sequence"]) + "_SQL"]
            scripts = [[insert_sql(item, k) for item in items] for k in range(ranges)]
            body, check = parallel(scripts, 7, "INSERT")
            timed_block(f"Insert into {names} in {ranges} parallel sessions (source-ordered id blocks)",
                        alloc_body + body)
            lines.append(check)
            report = []
            for item in items:
                dest = item["split"][0]
                report.append(
                    f"SELECT json_build_object('pgdm_expansion_timings', json_build_array(json_build_object("
                    f"'sequence', {next_sequence()}, 'stage', 'execute', "
                    f"'name', {sql_literal('Insert into ' + item['destination']['table_name'] + ' (statement ' + str(item['statement_index']) + ') - rows; time in parallel item above')}, "
                    f"'duration_seconds', 0, 'rows', (SELECT sum(rows) FROM {alloc} WHERE destination = {sql_literal(dest)} "
                    f"AND statement_index = {item['statement_index']} AND done), "
                    f"'destination', {sql_literal(item['destination']['table_name'])}, 'statement_index', {item['statement_index']})))::text;")
            lines += ["$PGDM_PSQL <<'PGDM_REPORT_" + str(state["sequence"]) + "_SQL' || exit 8", *report,
                      *[f"DROP TABLE IF EXISTS {t};" for item in items for t in item["stage_tables"]],
                      "PGDM_REPORT_" + str(state["sequence"]) + "_SQL"]
    return "\n".join(lines) + "\n", phases, stage_tables


def parse_pipeline_output(output: str):
    """Collect the manifest, all phase timing items and the staging timings."""
    manifest, items, concurrent = None, [], False
    for line in output.splitlines():
        candidate = line.strip()
        if candidate in ("PGDM_CONCURRENT_BEGIN", "PGDM_CONCURRENT_END"):
            concurrent = candidate.endswith("BEGIN")
            continue
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
            for item in parsed.get("pgdm_expansion_timings") or []:
                if concurrent:
                    item["include_in_total"] = False
                    item["name"] = f"{item.get('name')} (concurrent session; covered by the block above)"
                items.append(item)
    items.sort(key=lambda item: item.get("sequence") or 0)
    return manifest, items
