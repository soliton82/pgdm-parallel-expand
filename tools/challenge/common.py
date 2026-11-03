"""Shared helpers for the challenge benchmark / verification tools.

Connection settings come from environment variables only (no credentials in the repo):
  PGDM_SSH_HOST, PGDM_SSH_PORT (22), PGDM_SSH_USER, PGDM_SSH_PASSWORD,
  PGDM_PG_PORT (5432), PGDM_PG_USER, PGDM_PG_PASSWORD, PGDM_DATABASE (kaggle_challenge)
"""
import os
import shlex
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from services.postgres_service import PostgresAdminService  # noqa: E402

RECIPES = [("01_table3.sql", "public.table3"), ("02_table4.sql", "public.table4"),
           ("03_table1.sql", "public.table1"), ("04_table5.sql", "public.table5"),
           ("05_table6.sql", "public.table6"), ("06_table2.sql", "public.table2")]
DESTINATIONS = [t for _, t in RECIPES]


def env(name, default=None):
    value = os.environ.get(name, default)
    if value is None:
        raise SystemExit(f"Set {name} (see tools/challenge/README.md).")
    return value


def database() -> str:
    return env("PGDM_DATABASE", "kaggle_challenge")


def connect() -> PostgresAdminService:
    service = PostgresAdminService()
    service.connect(env("PGDM_SSH_HOST"), int(env("PGDM_SSH_PORT", "22")), env("PGDM_SSH_USER"),
                    env("PGDM_SSH_PASSWORD"), int(env("PGDM_PG_PORT", "5432")), env("PGDM_PG_USER"),
                    env("PGDM_PG_PASSWORD"))
    return service


def destinations_payload():
    return [{"table_name": table, "version_title": "Synthetic expansion",
             "sql": (ROOT / "SQL files" / name).read_text(encoding="utf-8")} for name, table in RECIPES]


def psql(service, database_name, sql) -> str:
    command = (f"psql -h localhost -U {shlex.quote(service.sql_username)} -d {shlex.quote(database_name)} "
               "-X -qAt -v ON_ERROR_STOP=1 -f -")
    return service.run_remote_command(command, stdin_text=sql)
