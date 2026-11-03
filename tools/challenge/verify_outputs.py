"""Digest the expansion result so two runs can be compared exactly.

Usage:
  python tools/challenge/verify_outputs.py save <file.json>
  python tools/challenge/verify_outputs.py compare <reference.json>

The digest covers every destination row INCLUDING generated identity values (md5 of all
columns ordered by primary key), the identity sequences, public.pgdm_table_row_counts and
the cross-table version/dependency rows in the control database (minus timestamps, users
and surrogate ids, which legitimately differ between runs).
"""
import json
import sys

from common import connect, database, psql
from config import CONTROL_DB, CONTROL_SCHEMA

TABLES = ["table3", "table4", "table1", "table5", "table6", "table2"]


def digest(service):
    db = database()
    out = {}
    for table in TABLES:
        out[table] = psql(service, db, f"SELECT count(*) || ':' || md5(coalesce(string_agg(x::text, E'\\n' "
                                       f"ORDER BY x.id_column_1), '')) FROM public.{table} x;").strip()
    out["sequences"] = psql(service, db, "SELECT string_agg(sequencename || '=' || coalesce(last_value::text, 'null'), ',' "
                                         "ORDER BY sequencename) FROM pg_sequences WHERE schemaname = 'public';").strip()
    out["row_counters"] = psql(service, db, "SELECT string_agg(schema_name || '.' || table_name || '=' || row_count, ',' "
                                            "ORDER BY table_name) FROM public.pgdm_table_row_counts;").strip()
    out["versions"] = psql(service, CONTROL_DB, f"""
SELECT string_agg(concat_ws('|', database_name, schema_name, table_name, version_code, version_title, md5(sql_recipe),
       raw_dump_path, raw_hash, raw_schema, operation_kind, version_history_format), E'\\n' ORDER BY table_name, version_code)
FROM {CONTROL_SCHEMA}.table_versions WHERE database_name = '{db}';""").strip()
    out["dependencies"] = psql(service, CONTROL_DB, f"""
SELECT string_agg(concat_ws('|', d.dependency_kind, d.source_database_name, d.source_schema_name, d.source_table_name,
       md5((d.dependency_payload - 'generated_at')::text)), E'\\n' ORDER BY d.dependency_kind, md5(d.dependency_payload::text))
FROM {CONTROL_SCHEMA}.table_version_dependencies d
JOIN {CONTROL_SCHEMA}.table_versions v ON v.id = d.table_version_id WHERE v.database_name = '{db}';""").strip()
    return out


def main():
    mode, path = sys.argv[1], sys.argv[2]
    current = digest(connect())
    if mode == "save":
        json.dump(current, open(path, "w"), indent=1)
        print(f"saved digest to {path}")
        return
    reference = json.load(open(path))
    different = [key for key in reference if reference[key] != current.get(key)]
    for key in reference:
        print(f"{'OK  ' if key not in different else 'DIFF'} {key}")
    print("IDENTICAL" if not different else f"DIFFERENT: {different}")
    sys.exit(1 if different else 0)


if __name__ == "__main__":
    main()
