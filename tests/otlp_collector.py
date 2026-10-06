# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""A stand-in OTLP/HTTP collector on a local port, for tests of `--otel-send`.

It records each request and answers as a test sets it to: a status, a
redirect, a slow answer, or a connection closed with no answer at all.
"""

import http.server
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Received:
    """One request the collector received."""

    path: str
    headers: dict[str, str]
    """Header names in lowercase, since HTTP does not distinguish their case."""
    body: bytes


@dataclass
class Collector:
    """How the collector answers, and what it has received."""

    url: str
    status: int = 200
    location: str | None = None
    delay: float = 0.0
    hang_up: bool = False
    received: list[Received] = field(default_factory=list)


def _handler(collector: Collector) -> type[http.server.BaseHTTPRequestHandler]:
    """Build a request handler that records into `collector` and answers as it says."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            headers = {name.lower(): value for name, value in self.headers.items()}
            collector.received.append(Received(self.path, headers, body))
            time.sleep(collector.delay)
            if collector.hang_up:
                self.close_connection = True
                return
            payload = b"{}"
            try:
                self.send_response(collector.status)
                if collector.location is not None:
                    self.send_header("Location", collector.location)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except OSError:
                # The client stopped waiting, as a timeout test means it to.
                self.close_connection = True

        def log_message(self, format: str, *args: object) -> None:
            """Keep request lines out of the test output."""

    return Handler


@contextmanager
def running_collector() -> Iterator[Collector]:
    """Serve a stand-in collector on a free local port until the block ends.

    Yields:
        Collector: Its URL, how it answers, and the requests it has received.
    """
    collector = Collector(url="")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _handler(collector))
    collector.url = f"http://127.0.0.1:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    try:
        yield collector
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
