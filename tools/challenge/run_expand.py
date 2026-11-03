"""Run the Expand operation headless, exactly as the GUI does, and print the timing report.

Usage: python tools/challenge/run_expand.py [--engine parallel|legacy]
"""
import os
import socket
import sys
import time

if "--engine" in sys.argv:
    os.environ["PDM_EXPAND_ENGINE"] = sys.argv[sys.argv.index("--engine") + 1]

from common import connect, database, destinations_payload, DESTINATIONS  # noqa: E402
from ui.dialogs import ExpansionTimingReportDialog  # noqa: E402


def main():
    service = connect()
    db = database()
    report = service.create_cross_table_expansion_timing_report(db, "public.raw_data", ["group001"], DESTINATIONS)
    with service.expansion_timing_scope(report, "prepare", "Ensure control database metadata"):
        service.ensure_admin_schema()
    result = service.execute_cross_table_expansion(db, "public.raw_data", ["group001"], destinations_payload(),
                                                   timing_report=report)
    service.register_cross_table_expansion_versions(expansion_result=result, requested_by="benchmark",
                                                    workstation_name=socket.gethostname(), timing_report=report)
    report["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    print(ExpansionTimingReportDialog.build_report_text(report))


if __name__ == "__main__":
    main()
