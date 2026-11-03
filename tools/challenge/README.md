# Challenge tools: benchmark, exact-output verification, crash/resume test

These tools use the application's own service layer: SSH, then `psql` on the Linux
server, exactly like the GUI. Credentials come from environment variables only.

```bash
export PGDM_SSH_HOST=<linux host> PGDM_SSH_PORT=22 PGDM_SSH_USER=<ssh user> PGDM_SSH_PASSWORD=<ssh password>
export PGDM_PG_PORT=5432 PGDM_PG_USER=<postgres role> PGDM_PG_PASSWORD=<postgres password>
export PGDM_DATABASE=kaggle_challenge
```

## 1. Benchmark and verify the output is identical
```bash
# fresh database from the official generator, then:
python tools/challenge/run_expand.py --engine legacy        # original single-transaction engine
python tools/challenge/verify_outputs.py save legacy.json

# regenerate the same database (same --rows), then:
python tools/challenge/run_expand.py                        # parallel engine (default)
python tools/challenge/verify_outputs.py compare legacy.json  # -> IDENTICAL
```
`run_expand.py` prints the same Expansion Performance Report as the GUI, including
`MEASURED PROCESSING TOTAL`.

## 2. Failure recovery
```bash
# fresh database, then kill the Expand after 10 s and let a second run resume it:
python tools/challenge/crash_resume_test.py legacy.json 10   # -> IDENTICAL
```
Repeat with other delays on a fresh database each time. The same applies in the GUI:
close the application, or cut the network, in the middle of an Expand. Then run the
same Expand again and it continues where it stopped.
