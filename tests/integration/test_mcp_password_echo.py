# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests that no reader of a password from `veridelta mcp`'s environment repeats one.

The server takes a database password, a Snowflake password, or a Snowflake
key's passphrase from a `${NAME}` as free text, and masks it only as written
and in a few encodings. Here each reader that can receive one is driven,
through the MCP client, with each wrong password of `password_corpus.CORPUS`,
against a fake server on this machine that answers as the real one refuses a
sign-in. Each HTTP service is driven twice: once as it refuses a sign-in, and
once as a service that writes its whole request back in the refusal, as an
echo or debug endpoint on an allowed host would. No answer may hold any piece
of the password, in any form. A control has the Postgres fake write the
password back in each escaped form, and shows the check finds each one.
Another shows why Trino's reader takes no user or password from the
environment: it sends both as basic auth, and repeats a service's whole
refusal.

Each call waits out ConnectorX's pool, about 30 seconds, so every call runs
in a thread of its own, at once.
"""

import base64
import datetime
import ipaddress
import json
import os
import socket
import ssl
import struct
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socketserver import BaseRequestHandler, ThreadingTCPServer
from typing import Any, Final

import anyio
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import NameOID
from mcp import Client

from tests.integration.password_corpus import CORPUS, escaped_forms, holds_piece
from veridelta import mcp_server
from veridelta.connectors import warehouse
from veridelta.connectors.database import connectorx_url
from veridelta.mcp_server import Settings, build_server
from veridelta.models import DatabaseConfig

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_TOOLS: Final[tuple[tuple[str, dict[str, Any]], ...]] = (
    ("validate_config", {"schemas": True}),
    ("describe_schema", {"side": "source"}),
    ("run_comparison", {}),
)
"""Each tool that reads a side, with the arguments it needs beside the path."""

_SENDS_NOTHING: Final = {
    "oracle": "no Oracle Client in CI",
    "snowflake-passphrase": "the key is decrypted on this machine",
}
"""The readers that send a fake nothing, and why, so a widening shows in review."""

_LONGEST_WAIT: Final = 120
"""The seconds a fake waits for a reader's next bytes before it gives up."""


@dataclass
class _Recording:
    """What a fake received, and the text its error repeats, shared by its handlers."""

    message: Callable[[bytes], str]
    """The text of the fake's refusal, given what the reader sent as its password,
    or its whole request to an HTTP fake."""

    received: list[bytes] = field(default_factory=list)
    """Each chunk of bytes a reader sent, in order."""


class _TcpFake(ThreadingTCPServer):
    """A fake of a binary protocol, which serves each connection in a thread."""

    daemon_threads = True
    recording: _Recording


class _HttpFake(ThreadingHTTPServer):
    """A fake of an HTTP service, which serves each request in a thread."""

    daemon_threads = True
    recording: _Recording


def _exactly(connection: socket.socket, size: int) -> bytes:
    """Read `size` bytes, or raise `EOFError` when the reader hangs up first."""
    data = b""
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


class _Socket(BaseRequestHandler):
    """Serve one connection of a binary protocol, and record what the reader sent."""

    server: Any

    def handle(self) -> None:
        """Run the exchange, and stop quietly when the reader hangs up."""
        self.request.settimeout(_LONGEST_WAIT)
        with suppress(EOFError, OSError):
            self.exchange()

    def exchange(self) -> None:
        """Answer the reader as the server refuses a sign-in."""
        raise NotImplementedError

    def read(self, size: int) -> bytes:
        """Read and record `size` bytes."""
        data = _exactly(self.request, size)
        self.server.recording.received.append(data)
        return data


class _Postgres(_Socket):
    """Ask for the password in clear text, then refuse it as PostgreSQL does."""

    def exchange(self) -> None:
        """Turn down TLS, ask for the password, and send an `ErrorResponse` 28P01."""
        while True:
            (size,) = struct.unpack("!i", self.read(4))
            body = self.read(size - 4)
            if body[:4] != struct.pack("!i", 80877103):
                break
            self.request.sendall(b"N")
        self.request.sendall(b"R" + struct.pack("!ii", 8, 3))
        self.read(1)
        (size,) = struct.unpack("!i", self.read(4))
        password = self.read(size - 4).rstrip(b"\0")
        text = self.server.recording.message(password).encode()
        fields = b"SFATAL\0VFATAL\0C28P01\0M" + text + b"\0\0"
        self.request.sendall(b"E" + struct.pack("!i", len(fields) + 4) + fields)


_MYSQL_CAPABILITIES: Final = 0x002BA20F
"""Protocol 4.1, secure and plugin sign-in, transactions, and no TLS."""


class _MySQL(_Socket):
    """Greet with `mysql_native_password`, then refuse the sign-in as MySQL does."""

    def _send(self, sequence: int, payload: bytes) -> None:
        """Send one packet."""
        self.request.sendall(len(payload).to_bytes(3, "little") + bytes([sequence]) + payload)

    def exchange(self) -> None:
        """Send a v10 handshake, read the reply, and send ERR 1045."""
        self._send(
            0,
            b"\x0a8.4.0\0"
            + struct.pack("<I", 7)
            + b"abcdefgh\0"
            + struct.pack("<H", _MYSQL_CAPABILITIES & 0xFFFF)
            + b"\xff"
            + struct.pack("<H", 2)
            + struct.pack("<H", _MYSQL_CAPABILITIES >> 16)
            + bytes([21])
            + bytes(10)
            + b"ijklmnopqrst\0"
            + b"mysql_native_password\0",
        )
        size = int.from_bytes(self.read(3), "little")
        self.read(1 + size)
        self._send(
            2,
            b"\xff"
            + struct.pack("<H", 1045)
            + b"#28000"
            + b"Access denied for user 'u'@'localhost' (using password: YES)",
        )


def _tds(kind: int, payload: bytes) -> bytes:
    """Wrap a payload in one TDS packet, the last of its message."""
    return struct.pack(">BBHHBB", kind, 1, 8 + len(payload), 0, 1, 0) + payload


def _ucs2(text: str) -> bytes:
    """Encode text as TDS writes it."""
    return text.encode("utf-16-le")


class _Tds(_Socket):
    """Turn down encryption, read LOGIN7, then refuse it as SQL Server does."""

    def _message(self) -> None:
        """Read one TDS message, packet by packet."""
        while True:
            header = self.read(8)
            self.read(struct.unpack(">H", header[2:4])[0] - 8)
            if header[1] & 1:
                return

    def exchange(self) -> None:
        """Answer PRELOGIN with ENCRYPT_NOT_SUP, then send ERROR 18456 and DONE."""
        self._message()
        options = b"\x00\x00\x0b\x00\x06" + b"\x01\x00\x11\x00\x01" + b"\xff"
        self.request.sendall(_tds(4, options + b"\x10\x00\x07\xd0\x00\x00" + b"\x02"))
        self._message()
        text = "Login failed for user 'u'."
        error = (
            struct.pack("<IBB", 18456, 1, 14)
            + struct.pack("<H", len(text))
            + _ucs2(text)
            + bytes([4])
            + _ucs2("fake")
            + bytes([0])
            + struct.pack("<I", 1)
        )
        done = b"\xfd" + struct.pack("<HHQ", 2, 0, 0)
        self.request.sendall(_tds(4, b"\xaa" + struct.pack("<H", len(error)) + error + done))


class _Http(BaseHTTPRequestHandler):
    """Refuse every request as a service refuses a sign-in, and record what it was sent."""

    server: Any
    status = 401
    content_type = "text/plain"

    def _refuse(self) -> None:
        """Record the request, then answer with the service's refusal."""
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        request = f"{self.command} {self.path}\n{self.headers}".encode() + body
        self.server.recording.received.append(request)
        data = self.server.recording.message(request).encode()
        self.send_response(self.status)
        self.send_header("Content-Type", self.content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        """Refuse a read."""
        self._refuse()

    def do_POST(self) -> None:
        """Refuse a query or a sign-in."""
        self._refuse()

    def log_message(self, format: str, *args: object) -> None:
        """Keep request lines out of the test output."""


class _Snowflake(_Http):
    """Answer a sign-in as Snowflake answers a wrong password."""

    status = 200
    content_type = "application/json"


@dataclass(frozen=True)
class _Reader:
    """A reader, the fake it signs in to, and the source that reaches it."""

    handler: type[BaseRequestHandler] | None
    """The fake's handler, or None for a reader that has no fake."""

    message: Callable[[bytes], str]
    """The text the fake refuses with, given what the reader sent as its password,
    or its whole request to an HTTP fake."""

    uri: str | None = None
    """A database side's `uri`, with the fake's port to fill in, or None for Snowflake."""

    read: str = "  table: t\n"
    """What a database side reads."""

    credential: str = "  password: ${{{variable}}}\n"
    """The settings that sign in, with the variable to fill in."""

    tls: bool = False
    """Whether the fake serves HTTPS."""

    def configuration(self, port: int, variable: str) -> str:
        """Write a configuration whose source reaches the fake at `port`, signing in with `variable`.

        A database side is compared with a file. A Snowflake side is compared
        with another table of the same account, since a comparison does not mix
        a warehouse with a file.
        """
        signs_in = self.credential.format(variable=variable)
        if self.uri is not None:
            side = f"  type: database\n  uri: {self.uri.format(port=port)}\n{self.read}{signs_in}"
            target = "  path: b.csv\n"
        else:
            # Each fake has a Snowflake account of its own, which names its port.
            side = (
                f"  type: snowflake\n  account: acct{port}\n  user: u\n  warehouse: W\n"
                f"  database: D\n  schema_name: S\n  table: T\n{signs_in}"
            )
            target = side.replace("table: T", "table: U")
        return f"source:\n{side}target:\n{target}primary_keys: [id]\n"


def _fixed(text: str) -> Callable[[bytes], str]:
    """Refuse with the same text whatever the reader sent."""
    return lambda _: text


def _echoed(text: str) -> Callable[[bytes], str]:
    """Refuse with `text`, then the whole request, as an echo or debug endpoint writes it."""
    return lambda request: f"{text} The request was:\n{request.decode('utf-8', 'replace')}"


_CLICKHOUSE_REFUSAL: Final = (
    "Code: 516. DB::Exception: u: Authentication failed: password is incorrect, or "
    "there is no user with such name. (AUTHENTICATION_FAILED)"
)
"""The text ClickHouse refuses a wrong password with."""

_TRINO_READ: Final = "  query: select 1 as id\n"
"""A Trino side reads through a query, since the server refuses a Trino table."""

_READERS: Final[dict[str, _Reader]] = {
    "postgres": _Reader(
        _Postgres,
        _fixed('password authentication failed for user "u"'),
        "postgres://u@127.0.0.1:{port}/x",
    ),
    # ConnectorX reads redshift:// as postgresql://, with the same reader.
    "redshift": _Reader(
        _Postgres,
        _fixed('password authentication failed for user "u"'),
        "redshift://u@127.0.0.1:{port}/x",
    ),
    "mysql": _Reader(_MySQL, _fixed(""), "mysql://u@127.0.0.1:{port}/x"),
    "mssql": _Reader(_Tds, _fixed(""), "mssql://u@127.0.0.1:{port}/x"),
    "clickhouse": _Reader(_Http, _fixed(_CLICKHOUSE_REFUSAL), "clickhouse://u@127.0.0.1:{port}/x"),
    "clickhouse-echo": _Reader(
        _Http, _echoed(_CLICKHOUSE_REFUSAL), "clickhouse://u@127.0.0.1:{port}/x"
    ),
    "oracle": _Reader(None, _fixed(""), "oracle://u@127.0.0.1:{port}/svc"),
    # The driver sends a password only to the account's host under snowflakecomputing.com,
    # never to another service an agent names, so it has no echoing variant.
    "snowflake-password": _Reader(
        _Snowflake,
        _fixed(
            json.dumps(
                {
                    "data": None,
                    "code": "390100",
                    "message": "Incorrect username or password was specified.",
                    "success": False,
                }
            )
        ),
    ),
    "snowflake-passphrase": _Reader(
        _Snowflake,
        _fixed("{}"),
        credential="  private_key_path: key.p8\n  private_key_passphrase: ${{{variable}}}\n",
    ),
}
"""Each reader that can receive a password from the server's environment, by name."""


@dataclass(frozen=True)
class _Case:
    """A reader, a password, and the variable that holds it."""

    reader: str
    password_id: str
    password: str
    variable: str


def _cases() -> list[_Case]:
    """Pair each reader with each password it is driven with, under its own variable."""
    pairs = [
        (reader, password_id, password) for reader in _READERS for password_id, password in CORPUS
    ]
    return [
        _Case(reader, password_id, password, f"V_PW_{number}")
        for number, (reader, password_id, password) in enumerate(pairs, start=1)
    ]


def _certificate(folder: Path) -> ssl.SSLContext:
    """Make a self-signed certificate for 127.0.0.1, and a context that serves it."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    (folder / "fake.pem").write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    (folder / "fake.key").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(folder / "fake.pem", folder / "fake.key")
    return context


@contextmanager
def _fake(reader: _Reader, tls: ssl.SSLContext | None) -> Iterator[tuple[int, _Recording]]:
    """Run a reader's fake on a free port of this machine until the block ends.

    Yields:
        tuple[int, _Recording]: Its port, and what it receives.
    """
    recording = _Recording(reader.message)
    if reader.handler is None:
        # Nothing listens on port 1, so a reader that did connect would fail at once.
        yield 1, recording
        return
    server_type = _HttpFake if issubclass(reader.handler, _Http) else _TcpFake
    server = server_type(("127.0.0.1", 0), reader.handler)
    server.recording = recording
    if reader.tls:
        assert tls is not None
        server.socket = tls.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    try:
        yield server.server_address[1], recording
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _answer(settings: Settings, path: str, tool: str, arguments: dict[str, Any]) -> str:
    """Call a tool through the client, on a server of its own, and join all it returned."""

    async def session() -> str:
        async with Client(build_server(settings)) as client:
            result = await client.call_tool(tool, {"path": path, **arguments})
        text = "".join(getattr(block, "text", "") for block in result.content)
        return json.dumps(result.structured_content, ensure_ascii=False) + text

    return anyio.run(session)


@pytest.fixture
def no_reader_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset each credential variable a reader takes from the server's environment.

    The server refuses to start on such a value it cannot mask, so a dummy the
    machine exports would fail every call before it reaches a reader.
    """
    left_out = {
        *mcp_server._ENVIRONMENT_SLOTS,  # pyright: ignore[reportPrivateUsage]  # the lists are private to the server
        *mcp_server._TOKEN_FILE_VARIABLES,  # pyright: ignore[reportPrivateUsage]  # as above
        mcp_server._KEY_JSON_VARIABLE,  # pyright: ignore[reportPrivateUsage]  # as above
    }
    for name in list(os.environ):
        if mcp_server._name_as_read(name) in left_out:  # pyright: ignore[reportPrivateUsage]  # as above
            monkeypatch.delenv(name)


@pytest.fixture
def snowflake_ports(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Send each Snowflake sign-in to the fake whose port its account names, over HTTP.

    Returns:
        dict[str, int]: Each account's port, which a test fills in.
    """
    ports: dict[str, int] = {}
    driver = warehouse.snowflake_connector
    connect = driver.connect

    def to_the_fake(**kwargs: Any) -> Any:
        port = ports[kwargs["account"]]
        return connect(**kwargs, host="127.0.0.1", port=port, protocol="http", login_timeout=10)

    monkeypatch.setattr(driver, "connect", to_the_fake)
    return ports


def _settings(root: Path, ports: dict[str, int]) -> Settings:
    """Allow this machine and each fake Snowflake account, row values, and queries."""
    accounts = tuple(f"{account}.snowflakecomputing.com" for account in ports)
    return Settings(
        (root,),
        allow_row_values=True,
        allow_queries=True,
        allowed_hosts=("127.0.0.1", "localhost", *accounts),
    )


def _encrypted_key(folder: Path) -> None:
    """Write an RSA key in PKCS8, encrypted with a passphrase no case gives."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    (folder / "key.p8").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(b"Correct-Q7-Passphrase"),
        )
    )


def _run_all(tasks: list[tuple[str, str, dict[str, Any]]], settings: Settings) -> list[str]:
    """Run each (path, tool, arguments) at once, in a thread of its own, and return each answer."""
    with ThreadPoolExecutor(max_workers=min(len(tasks), 64)) as pool:
        futures = [pool.submit(_answer, settings, *task) for task in tasks]
        return [future.result() for future in futures]


def test_no_reader_repeats_any_piece_of_a_password(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    no_reader_credentials: None,
    snowflake_ports: dict[str, int],
) -> None:
    """Ensure no answer holds a run of 4 characters, or an escaped form, of a wrong password.

    Each case's fake must also have received bytes from its reader, so the
    reader got past reading its configuration, unless `_SENDS_NOTHING` says
    why it sends nothing. An echoing fake's answer must hold the request it
    wrote back, with the header that carries the password masked.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "b.csv").write_text("id\n1\n", encoding="utf-8")
    _encrypted_key(tmp_path)
    cases = _cases()
    with ExitStack() as stack:
        tasks: list[tuple[str, str, dict[str, Any]]] = []
        recordings: list[_Recording] = []
        for case in cases:
            monkeypatch.setenv(case.variable, case.password)
            reader = _READERS[case.reader]
            for tool, arguments in _TOOLS:
                port, recording = stack.enter_context(_fake(reader, None))
                snowflake_ports[f"acct{port}"] = port
                path = f"{case.variable}-{tool}.yaml"
                (tmp_path / path).write_text(
                    reader.configuration(port, case.variable),
                    encoding="utf-8",
                )
                tasks.append((path, tool, arguments))
                recordings.append(recording)
        answers = _run_all(tasks, _settings(tmp_path, snowflake_ports))

    pairs = [(case, tool) for case in cases for tool, _ in _TOOLS]
    for (case, tool), answer, recording in zip(pairs, answers, recordings, strict=True):
        where = f"{case.reader}, {case.password_id}, {tool}"
        found = holds_piece(answer, case.password)
        assert found is None, f"{where}: the answer holds {found}: {answer}"
        assert bool(recording.received) is (case.reader not in _SENDS_NOTHING), where
        if case.reader.endswith("-echo"):
            # The refusal came back with the request in it, the password's header masked.
            assert "x-clickhouse-key: ***" in answer.casefold(), f"{where}: {answer}"


def test_the_corpus_drives_every_password_reader() -> None:
    """Ensure the database readers here are the ones the server takes a password for."""
    readers = {
        connectorx_url(DatabaseConfig(uri=reader.uri.format(port=1), table="t")).scheme
        for reader in _READERS.values()
        if reader.uri is not None
    }

    assert readers == mcp_server._PASSWORD_READERS  # pyright: ignore[reportPrivateUsage]  # the list is private to the server


def _echoing(form: str) -> Callable[[bytes], str]:
    """Refuse with the password the reader sent, written in an escaped form."""
    return lambda password: (
        "password authentication failed: "
        + escaped_forms(password.decode()).get(form, password.decode())
    )


def test_the_check_finds_each_escaped_echo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, no_reader_credentials: None
) -> None:
    """Ensure `holds_piece` finds each escaped form of a password a server writes back.

    The fake writes the password it received back in each form the mask does
    not match, and once as received, which the mask turns into `***`. So the
    check has teeth, and an escaped echo would come back unmasked.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "b.csv").write_text("id\n1\n", encoding="utf-8")
    password = dict(CORPUS)["every-class"]
    monkeypatch.setenv("V_PW_ECHO", password)
    forms = [*escaped_forms(password), "as received"]
    with ExitStack() as stack:
        tasks: list[tuple[str, str, dict[str, Any]]] = []
        for number, form in enumerate(forms):
            reader = _READERS["postgres"]
            echo = _Reader(reader.handler, _echoing(form), reader.uri)
            port, _ = stack.enter_context(_fake(echo, None))
            path = f"echo-{number}.yaml"
            (tmp_path / path).write_text(
                echo.configuration(port, "V_PW_ECHO"),
                encoding="utf-8",
            )
            tasks.append((path, "describe_schema", {"side": "source"}))
        answers = _run_all(tasks, _settings(tmp_path, {}))

    *escaped, received = answers
    for form, answer in zip(forms[:-1], escaped, strict=True):
        assert holds_piece(answer, password) is not None, form
    assert "password authentication failed: ***" in received
    assert holds_piece(received, password) is None


def test_the_trino_reader_repeats_a_password_a_service_writes_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, no_reader_credentials: None
) -> None:
    """Ensure the reason Trino's reader takes no user or password from the environment still holds.

    The reader sends the user and the password as basic auth, in base64, which
    the mask does not match, and its error repeats the whole body of a
    refusal. So a service that writes its request headers back hands both
    over. Both are written in the file here, since the server refuses either
    from its environment for Trino.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "b.csv").write_text("id\n1\n", encoding="utf-8")
    user, password = "Vq7-Jx.Wz_Kp", "QzXj-Vk_Wq.Zx"
    trino = _Reader(
        _Http,
        _echoed("Unauthorized."),
        f"trino+https://{user}@127.0.0.1:{{port}}/c?verify=false",
        _TRINO_READ,
        credential=f"  password: {password}\n",
        tls=True,
    )
    with _fake(trino, _certificate(tmp_path)) as (port, recording):
        (tmp_path / "trino.yaml").write_text(trino.configuration(port, "unused"), encoding="utf-8")
        (answer,) = _run_all([("trino.yaml", "run_comparison", {})], _settings(tmp_path, {}))

    assert recording.received
    assert base64.b64encode(f"{user}:{password}".encode()).decode() in answer
