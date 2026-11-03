"""Failure-recovery test: kill the DB Manager in the middle of an Expand, run it again, verify.

Usage: python tools/challenge/crash_resume_test.py <reference_digest.json> <kill_after_seconds> [<kill_after_seconds> ...]

Prerequisite: a fresh benchmark database (official generator) for EACH kill time, and a
reference digest saved from an uninterrupted run on the same data
(`verify_outputs.py save ref.json`). For a single kill time the script:
  1. starts `run_expand.py` in a child process,
  2. kills it with SIGKILL after the given number of seconds (the SSH channel drops; the
     server-side sessions the run started are left behind on purpose),
  3. runs `run_expand.py` again -- it finds the unfinished job in public.pgdm_expansion_jobs,
     stops the orphaned sessions, skips completed steps and finishes the job,
  4. compares the final database with the reference digest.
"""
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    reference, kill_after = sys.argv[1], float(sys.argv[2])
    child = subprocess.Popen([sys.executable, str(HERE / "run_expand.py")],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(kill_after)
    if child.poll() is None:
        child.kill()
        print(f"killed the Expand after {kill_after:.1f} s")
    else:
        print("the Expand finished before the kill time; choose a shorter delay")
    child.wait()
    resumed = subprocess.run([sys.executable, str(HERE / "run_expand.py")], capture_output=True, text=True)
    if resumed.returncode != 0:
        print(resumed.stdout[-2000:], resumed.stderr[-2000:])
        sys.exit("the resumed Expand failed")
    total = next((line for line in resumed.stdout.splitlines() if "MEASURED PROCESSING TOTAL" in line), "")
    print(f"resumed Expand finished. {total.strip()}")
    sys.exit(subprocess.run([sys.executable, str(HERE / "verify_outputs.py"), "compare", reference]).returncode)


if __name__ == "__main__":
    main()
