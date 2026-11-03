"""One long-lived ``psql`` per database for the read-only catalog checks of Expand.

Expand's preflight issues ~20 small read-only queries (table existence, column
metadata, row counters, dependency checks). The legacy path starts a new
``psql`` process for each one, so most of every check is process start-up,
connection and authentication on the server. Inside an Expand, those one-shot
``psql ... -c <sql>`` reads are sent to a persistent ``psql`` session instead
(one SSH channel per database, opened on first use, closed when the operation
ends). Results, errors and output format (unaligned, tuples only) are the same.

Timing stays server-side and honest: each query's time is taken from psql's own
``\\timing`` on the Linux host and recorded like the legacy remote timing.
"""
from __future__ import annotations

import re
import shlex
import threading
import time
import uuid

_TIME_RE = re.compile(r"^Time: ([0-9.]+) ms(?: \(.*\))?$")


_FORMATS = {
    ("-At",): ("unaligned", "on"),
    ("-qAt",): ("unaligned", "on"),
    ("--csv",): ("csv", "off"),
    ("-q",): ("aligned", "off"),
}


def parse_psql_command(command: str, stdin_text=None):
    """Recognise the app's own one-shot psql invocations that a persistent session can serve.

    Returns (database, sql, output_format, tuples_only) or None. Accepted:
      * ``psql -h localhost -U u -d db -X <-At|-qAt|--csv> [-v ON_ERROR_STOP=1] -c "<SELECT|WITH ...>"``
        (single read-only statement), and
      * ``psql -h localhost -U u -d db -X <-q|-qAt> -v ON_ERROR_STOP=1 [-f -]`` with the script on stdin
        (ON_ERROR_STOP semantics are kept: the session stops at the first error).
    """
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    if len(tokens) < 8 or tokens[0] != "psql" or tokens[1:3] != ["-h", "localhost"] or tokens[3] != "-U" \
            or tokens[5] != "-d" or tokens[7] != "-X":
        return None
    database, rest = tokens[6], tokens[8:]
    if stdin_text is None:
        if len(rest) < 3 or rest[-2] != "-c":
            return None
        flags, sql = rest[:-2], rest[-1]
        if flags[1:] not in ([], ["-v", "ON_ERROR_STOP=1"]):
            return None
        if not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE) or ";" in sql.strip().rstrip(";"):
            return None
    else:
        if not isinstance(stdin_text, str):
            return None
        if rest[-2:] == ["-f", "-"]:
            rest = rest[:-2]
        if rest[1:] != ["-v", "ON_ERROR_STOP=1"]:
            return None
        flags, sql = rest[:1], stdin_text
    fmt = _FORMATS.get(tuple(flags[:1]))
    if not fmt:
        return None
    return database, sql, fmt[0], fmt[1]


class PersistentPsql:
    """A psql process on the server, driven over one SSH channel."""

    def __init__(self, ssh_client, username, password, port, database):
        self._lock = threading.Lock()
        self._channel = ssh_client.get_transport().open_session()
        self._channel.exec_command(
            f"export PGPASSWORD={shlex.quote(password)} PGPORT={shlex.quote(str(port))}; "
            f"exec psql -h localhost -U {shlex.quote(username)} -d {shlex.quote(database)} "
            "-X -q -A -t -v ON_ERROR_STOP=1"
        )
        self._buffer = ""
        self._stderr = ""
        self._send("\\timing on\n")
        self.query("SELECT 1;")

    def _send(self, text):
        self._channel.sendall(text.encode("utf-8"))

    def _read_until(self, marker, timeout=600.0):
        deadline = time.monotonic() + timeout
        while marker not in self._buffer:
            if self._channel.recv_stderr_ready():
                self._stderr += self._channel.recv_stderr(65536).decode("utf-8", "replace")
            if self._channel.recv_ready():
                self._buffer += self._channel.recv(65536).decode("utf-8", "replace")
                continue
            if self._channel.exit_status_ready() and not self._channel.recv_ready():
                raise RuntimeError("Persistent psql session ended unexpectedly: " + self._stderr.strip())
            if time.monotonic() > deadline:
                raise TimeoutError("Persistent psql session did not answer in time.")
            time.sleep(0.001)
        head, _, rest = self._buffer.partition(marker)
        line, _, self._buffer = rest.partition("\n")
        while self._channel.recv_stderr_ready():
            self._stderr += self._channel.recv_stderr(65536).decode("utf-8", "replace")
        return head, line

    def query(self, sql, output_format="unaligned", tuples_only="on"):
        """Run `sql`; return (stdout_text, server_seconds).

        The session runs with ON_ERROR_STOP=1, exactly like the one-shot commands it
        replaces: on the first error psql exits, the error is raised and the caller
        must drop this session."""
        with self._lock:
            marker = f"__PGDM_PSQL_DONE_{uuid.uuid4().hex}__"
            self._stderr = ""
            self._send(f"\\pset format {output_format}\n\\pset tuples_only {tuples_only}\n"
                       + sql.rstrip() + ("\n" if sql.rstrip().endswith(";") else ";\n")
                       + f"\\echo {marker} :ERROR\n")
            head, status = self._read_until(marker)
            seconds, lines = 0.0, []
            for line in head.splitlines():
                timing = _TIME_RE.match(line.strip())
                if timing:
                    seconds += float(timing.group(1)) / 1000.0
                else:
                    lines.append(line)
            if status.strip().lower() == "true":
                raise RuntimeError(self._stderr.strip() or "psql reported an error.")
            return "\n".join(lines).strip(), seconds

    def close(self):
        try:
            self._send("\\q\n")
        except Exception:
            pass
        try:
            self._channel.close()
        except Exception:
            pass
