import json
import unittest
from unittest.mock import patch

from services import parallel_expansion
from services.postgres_service import PostgresAdminService


class ParallelExpansionTests(unittest.TestCase):
    SOURCE = parallel_expansion.source_relation_sql("public", "raw_data", ["group001"])

    def test_row_local_recipe_is_staged_and_distinct_recipe_is_not(self):
        row_local = f"INSERT INTO public.table1 (a, b) SELECT s.raw_id, 1 FROM {self.SOURCE} AS s"
        distinct = f"INSERT INTO public.table3 (a) SELECT DISTINCT s.raw_id FROM {self.SOURCE} AS s"
        dest, columns, select_sql = parallel_expansion.split_statement(row_local, self.SOURCE)
        self.assertEqual((dest, columns), ("public.table1", ["a", "b"]))
        self.assertIn("s.raw_id AS pgdm_order_key", select_sql)
        self.assertIsNone(parallel_expansion.split_statement(distinct, self.SOURCE))

    def test_phases_follow_read_dependencies(self):
        def dest(name, sql, reads=()):
            return {"table_name": f"public.{name}", "schema_name": "public", "pure_table_name": name,
                    "statements": [sql], "read_relations": list(reads)}
        src = self.SOURCE
        destinations = [
            dest("table3", f"INSERT INTO public.table3 (a) SELECT DISTINCT 1 FROM {src} AS s"),
            dest("table1", f"INSERT INTO public.table1 (a) SELECT s.raw_id FROM {src} AS s JOIN public.table3 t ON true",
                 [("public", "table3")]),
            dest("table5", f"INSERT INTO public.table5 (a) SELECT m.id FROM {src} AS s JOIN public.table1 m ON true",
                 [("public", "table1")]),
            dest("table6", f"INSERT INTO public.table6 (a) SELECT m.id FROM {src} AS s JOIN public.table1 m ON true",
                 [("public", "table1")]),
        ]
        phases = parallel_expansion.plan_phases(destinations, src)
        self.assertEqual([[i["destination"]["pure_table_name"] for i in p] for p in phases],
                         [["table3"], ["table1"], ["table5", "table6"]])

    def test_pipeline_output_merges_phase_and_stage_timings(self):
        output = "\n".join([
            json.dumps({"raw_hash_manifest": [], "expected_row_count": 3}),
            'PGDM_STAGE_TIMING 1500000000 {"sequence": 12, "name": "Transform"}',
            json.dumps({"pgdm_expansion_timings": [{"sequence": 11, "name": "Insert", "duration_seconds": 0.5,
                                                    "rows": 3, "destination": "public.table1"}]}),
            "PGDM_CONCURRENT_BEGIN",
            json.dumps({"pgdm_expansion_timings": [{"sequence": 13, "name": "x", "duration_seconds": 0.1}]}),
            "PGDM_CONCURRENT_END",
        ])
        manifest, items = parallel_expansion.parse_pipeline_output(output)
        self.assertEqual(manifest["expected_row_count"], 3)
        self.assertEqual([i["sequence"] for i in items], [11, 12, 13])
        self.assertAlmostEqual(items[1]["duration_seconds"], 1.5)
        self.assertFalse(items[2]["include_in_total"])

    def test_pipeline_script_is_one_bash_program(self):
        service = PostgresAdminService()
        service.sql_username = "kpg"
        src = self.SOURCE
        destinations = [{"table_name": "public.table1", "schema_name": "public", "pure_table_name": "table1",
                         "statements": [f"INSERT INTO public.table1 (a) SELECT s.raw_id FROM {src} AS s"],
                         "read_relations": []}]
        script, phases, stages = parallel_expansion.build_pipeline_script(
            service, database_name="db", source_schema_name="public", source_table_name="raw_data",
            raw_schemas=["group001"], prepared_destinations=destinations, ranges=3,
            timing_table_sql=service._EXPANSION_TIMING_TABLE_SQL, timing_result_sql=service._EXPANSION_TIMING_RESULT_SQL,
            manifest_sql="SELECT 1;", job_id="job-1", tag="abcd1234", scope_key="scope")
        self.assertEqual(len(stages), 3)
        self.assertEqual(script.count("CREATE UNLOGGED TABLE public.pgdm_xstage_"), 3)
        self.assertIn("CREATE TABLE IF NOT EXISTS public.pgdm_xstage_abcd1234_alloc", script)
        # every staging range, the allocation and the manifest are journalled and guarded
        self.assertEqual(script.count("\\if :pgdm_step_done"), 3 + 1 + 1)  # stages, allocation, manifest
        self.assertIn("done_at > pg_postmaster_start_time()", script)
        self.assertIn("status = 'data_committed'", script)
        self.assertIn("OVERRIDING SYSTEM VALUE", script)
        self.assertIn("PGDM_INSERT_", script)  # default: parallel insert sessions

    def _two_phase_script(self, bulk_load):
        service = PostgresAdminService()
        service.sql_username = "kpg"
        src = self.SOURCE
        destinations = [
            {"table_name": "public.table3", "schema_name": "public", "pure_table_name": "table3",
             "statements": [f"INSERT INTO public.table3 (a) SELECT DISTINCT s.raw_id FROM {src} AS s"],
             "read_relations": []},
            {"table_name": "public.table1", "schema_name": "public", "pure_table_name": "table1",
             "statements": [f"INSERT INTO public.table1 (a) SELECT d.a FROM {src} AS s JOIN public.table3 AS d ON d.a = s.raw_id"],
             "read_relations": [("public", "table3")]},
        ]
        return parallel_expansion.build_pipeline_script(
            service, database_name="db", source_schema_name="public", source_table_name="raw_data",
            raw_schemas=["group001"], prepared_destinations=destinations, ranges=2,
            timing_table_sql=service._EXPANSION_TIMING_TABLE_SQL, timing_result_sql=service._EXPANSION_TIMING_RESULT_SQL,
            manifest_sql="SELECT 1;", job_id="job-1", tag="abcd1234", scope_key="scope", bulk_load=bulk_load)[0]

    def test_bulk_load_is_one_guarded_transaction_that_restores_the_schema(self):
        script = self._two_phase_script(bulk_load=True)
        self.assertNotIn("PGDM_INSERT_", script)
        self.assertEqual(script.count("<<'PGDM_BULK_"), 1)
        bulk = script[script.index("<<'PGDM_BULK_"):]
        bulk = bulk[:bulk.index("PGDM_BULK_", 20)]
        # journal guard + the journal row inside the same transaction as the data
        self.assertIn("step_key = 'bulk:public.table1:1'", bulk)
        self.assertIn("VALUES ('job-1', 'bulk:public.table1:1')", bulk)
        self.assertEqual(bulk.count("BEGIN;"), 1)
        self.assertEqual(bulk.count("COMMIT;"), 1)
        # constraints are only dropped for an empty, owned table and re-created from the catalog
        for needle in ("pg_has_role(current_user, c.relowner, 'USAGE')", "SELECT EXISTS (SELECT 1 FROM %s)",
                       "pg_get_constraintdef(c.oid)", "pg_get_indexdef(i.indexrelid)",
                       "DROP CONSTRAINT %I", "ADD CONSTRAINT %I %s", "OVERRIDING SYSTEM VALUE"):
            self.assertIn(needle, bulk)
        self.assertLess(bulk.index("$pgdm_bulk_drop$"), bulk.index("$pgdm_insert$"))
        self.assertLess(bulk.rindex("$pgdm_insert$"), bulk.index("$pgdm_bulk_rebuild$"))
        self.assertLess(bulk.index("$pgdm_bulk_rebuild$"), bulk.index("COMMIT;"))

    def test_fields_are_decoded_once_only_where_it_pays(self):
        src, rng = self.SOURCE, "(SELECT * FROM public.raw_data WHERE raw_id < 10)"
        many = (f"SELECT s.raw_id AS k, s.raw -> 'values' ->> 'a', s.raw -> 'values' ->> 'b', "
                f"s.raw -> 'values' ->> 'a' FROM {src} AS s")
        out = parallel_expansion.decode_fields_once(many, src, rng)
        self.assertIn("jsonb_to_record(CASE WHEN jsonb_typeof(pgdm_s.raw -> 'values') = 'object'", out)
        self.assertIn('AS pgdm_d("a" text, "b" text) ON true', out)
        self.assertEqual(out.count('s."pgdm_dec_a"'), 2)
        self.assertNotIn("s.raw -> 'values' ->> ", out)
        self.assertIn(f"FROM {rng} AS pgdm_s", out)
        join = (f"SELECT d.id FROM {src} AS s JOIN public.dim AS d ON d.code = UPPER(s.raw -> 'values' ->> 'c') "
                f"WHERE s.raw_id > 0")
        self.assertIn('s."pgdm_dec_c"', parallel_expansion.decode_fields_once(join, src, rng))
        few = f"SELECT s.raw -> 'values' ->> 'a' FROM {src} AS s WHERE (s.raw -> 'values' ->> 'a') <> ''"
        self.assertEqual(parallel_expansion.decode_fields_once(few, src, rng), few.replace(src, rng))
        self.assertEqual(parallel_expansion.decode_fields_once(many, src, rng, enabled=False), many.replace(src, rng))

    def test_statistics_are_refreshed_for_tables_read_by_a_later_phase(self):
        script = self._two_phase_script(bulk_load=False)
        self.assertEqual(script.count("<<'PGDM_ANALYZE_SQL'"), 1)
        self.assertIn("ARRAY['public.table3']::text[]", script)
        self.assertLess(script.index("PGDM_ANALYZE_SQL"), script.index("CREATE UNLOGGED TABLE public.pgdm_xstage_"))


if __name__ == "__main__":
    unittest.main()
