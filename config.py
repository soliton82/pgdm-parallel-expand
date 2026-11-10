import os


APP_NAME = "PostgreSQL Data Manager"
APP_ID = "postgres_data_manager"


def _environment_value(name: str, default: str) -> str:
    return str(os.getenv(name) or default).strip() or default


# Public defaults can be overridden without editing the source code.
CONTROL_DB = _environment_value("PDM_CONTROL_DB", "postgres_data_manager")
CONTROL_SCHEMA = _environment_value("PDM_CONTROL_SCHEMA", "app_control")
LEGACY_CONTROL_SCHEMA = _environment_value("PDM_LEGACY_CONTROL_SCHEMA", "app_control_legacy")
DATA_DICTIONARY_TABLE = "data_dictionary"
DATA_DICTIONARY_USAGE_TABLE = "data_dictionary_usage"

REMOTE_STORAGE_DIR = _environment_value("PDM_REMOTE_STORAGE_DIR", "postgres_data_manager_storage")
LOCAL_STORAGE_DIR = _environment_value("PDM_LOCAL_STORAGE_DIR", APP_NAME)

APP_TITLE = _environment_value("PDM_APP_TITLE", APP_NAME)
APP_GEOMETRY = "1560x860"

# Expand engine: "parallel" (phase-committed, range-parallel staging; default) or "legacy"
# (the original single-transaction script). PDM_EXPAND_RANGES: number of server sessions,
# "auto" = number of server CPU cores (capped at 16).
EXPAND_ENGINE = _environment_value("PDM_EXPAND_ENGINE", "parallel").lower()
EXPAND_RANGES = _environment_value("PDM_EXPAND_RANGES", "auto").lower()
# Target source rows per chunk; workers process chunks round-robin (PDM_EXPAND_CHUNK_ROWS).
EXPAND_CHUNK_ROWS = max(10000, int(_environment_value("PDM_EXPAND_CHUNK_ROWS", "250000")))
# Load each phase's staged rows in ONE transaction that drops the destination's FKs / UNIQUE
# constraints / secondary indexes when it is empty and re-creates + re-validates them before
# COMMIT ("on", default), or insert with N parallel sessions and row-by-row checks ("off").
EXPAND_BULK_LOAD = _environment_value("PDM_EXPAND_BULK_LOAD", "on").lower() not in ("0", "off", "false", "no")
# Decode a staged recipe's raw -> 'values' ->> 'key' fields once per source row with
# jsonb_to_record ("on", default) or evaluate every reference as written ("off").
EXPAND_DECODE_FIELDS = _environment_value("PDM_EXPAND_DECODE_FIELDS", "on").lower() not in ("0", "off", "false", "no")
