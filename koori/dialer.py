import asyncio
import contextlib
import logging
import os
import tempfile

from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Protocol

import asyncssh

from .metric import COLLECTORS

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterable, Awaitable


class StreamReader(Protocol):
    __slots__ = ()

    async def readline(self) -> bytes: ...
    async def read(self, n: int = -1) -> bytes: ...
    async def readexactly(self, n: int) -> bytes: ...


class StreamWriter(Protocol):
    __slots__ = ()

    def write(self, data: bytes) -> None: ...
    def close(self) -> None: ...
    async def drain(self) -> None: ...
    async def wait_closed(self) -> None: ...


TMP_DIR = os.path.join(tempfile.gettempdir(), __package__ or __name__)
JOURNAL_CMD = ('journalctl', '-f', '-n', '15', '-o', 'short', '--no-pager')
METRICS_PORT = 9100

log = logging.getLogger(__name__)


def _build_ssh_config_files() -> tuple[str, ...]:
    if _config := os.environ.get('SSH_CONFIG'):
        log.info('using SSH config: %s', _config)
        return (os.path.expanduser(_config),)
    if os.path.exists(path := os.path.expanduser('~/.ssh/config')):
        log.info('using SSH config: %s', path)
        return (path,)
    return ()


SSH_CONFIG_FILES = _build_ssh_config_files()

REQUEST = (
    'GET /metrics?'
    + '&'.join('collect%5B%5D=' + c for c in COLLECTORS)
    + ' HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n'
).encode('ascii')


async def _read_http_response(reader: StreamReader) -> bytes:
    status_line = await reader.readline()
    log.debug('status: %r', status_line)
    if not status_line.startswith((b'HTTP/1.1 200', b'HTTP/1.0 200')):
        raise RuntimeError(f'HTTP status error: {status_line.decode('latin-1', 'replace').strip()}')

    # 1. Read headers until b'\r\n'
    content_length = None
    chunked = False
    while True:
        line = await reader.readline()
        log.debug('read header line: %r', line)
        if not line:
            raise EOFError('Connection closed while reading headers')
        if line == b'\r\n':
            break
        if line:
            # Parse headers for Content-Length or chunked transfer encoding
            key, _, value = line.partition(b':')
            key = key.strip().lower()
            value = value.strip()
            if key == b'content-length':
                content_length = int(value)
            elif key == b'transfer-encoding' and b'chunked' in value.lower():
                chunked = True

    if chunked:
        decoded_body = bytearray()
        while True:
            size_line = await reader.readline()
            if not size_line:
                raise EOFError('Connection closed while reading chunk size')
            size_hex = size_line.partition(b';')[0].strip()
            chunk_size = int(size_hex, 16)

            if chunk_size == 0:
                # Consume trailer headers and the final empty line
                while True:
                    trailer = await reader.readline()
                    if trailer == b'\r\n' or not trailer:
                        break
                break

            try:
                decoded_body.extend(await reader.readexactly(chunk_size))
            except asyncio.IncompleteReadError as e:
                raise EOFError('Connection closed while reading chunk data') from e

            # Consume the trailing CRLF of the chunk
            crlf = await reader.readline()
            if crlf != b'\r\n':
                log.warning('Chunk data not followed by CRLF: %r', crlf)

        return bytes(decoded_body)

    if content_length is not None:
        try:
            return await reader.readexactly(content_length)
        except asyncio.IncompleteReadError as e:
            raise EOFError('Connection closed while reading exact body') from e

    # HTTP/1.0 fallback
    return await reader.read()


type StreamPair = tuple[StreamReader, StreamWriter]


class HTTPConnection:
    __slots__ = ('_reader', '_writer')

    _reader: StreamReader
    _writer: StreamWriter

    def __init__(self, stream: StreamPair) -> None:
        self._reader, self._writer = stream

    async def request(self, request: bytes) -> bytes:
        self._writer.write(request)
        await self._writer.drain()
        return await _read_http_response(self._reader)

    async def close(self) -> None:
        self._writer.close()
        with contextlib.suppress(OSError):
            await self._writer.wait_closed()


class Session(ABC):
    __slots__ = ('_conn',)

    _conn: HTTPConnection | None

    def __init__(self) -> None:
        self._conn = None

    @abstractmethod
    def journal_lines(self) -> AsyncIterable[str]: ...

    @abstractmethod
    def _connect_stream(self) -> Awaitable[tuple[StreamReader, StreamWriter]]: ...

    @abstractmethod
    async def run_command(self, cmd: str) -> str: ...

    async def fetch_metrics(self, name: str) -> str:
        # Self-healing retry loop: retry once on a fresh connection if the persistent one was closed
        for attempt in range(2):
            try:
                if self._conn is None:
                    log.debug('%s: connecting (attempt %d)', name, attempt)
                    self._conn = HTTPConnection(await self._connect_stream())

                data = await self._conn.request(REQUEST)
                break
            except (OSError, EOFError, RuntimeError, ValueError) as e:
                log.exception('%s: http error (attempt %d)', name, attempt)
                await self.close_metrics(name)
                err = e
        else:
            assert err  # pyright: ignore[reportPossiblyUnboundVariable]
            raise err

        assert data  # pyright: ignore[reportPossiblyUnboundVariable]
        log.debug('%s: metrics: %d bytes', name, len(data))

        if log.isEnabledFor(logging.DEBUG):
            os.makedirs(TMP_DIR, exist_ok=True)
            with open(os.path.join(TMP_DIR, f'metrics-{name}.log'), 'wb') as fp:
                fp.write(data)

        return data.decode('utf-8', errors='replace')

    async def close_metrics(self, name: str) -> None:
        if self._conn is not None:
            log.warning('%s: metrics disconnecting', name)
            await self._conn.close()
            self._conn = None


class Dialer(ABC):
    __slots__ = ()

    @abstractmethod
    def open(self, name: str) -> AbstractAsyncContextManager[Session]: ...


class LocalSession(Session):
    __slots__ = ('_stdout',)

    def __init__(self, stdout: asyncio.StreamReader) -> None:
        super().__init__()
        self._stdout = stdout

    async def journal_lines(self) -> AsyncIterable[str]:
        while line := await self._stdout.readline():
            yield line.decode('utf-8', errors='replace')

    def _connect_stream(self) -> Awaitable[StreamPair]:
        return asyncio.open_connection('127.0.0.1', METRICS_PORT)

    async def run_command(self, cmd: str) -> str:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
        return (stdout or stderr).decode('utf-8', errors='replace')


class LocalDialer(Dialer):
    __slots__ = ()

    @asynccontextmanager
    async def open(self, name: str) -> AsyncGenerator[Session]:
        proc = await asyncio.create_subprocess_exec(
            *JOURNAL_CMD, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        assert proc.stdout is not None
        conn = LocalSession(proc.stdout)
        try:
            yield conn
        finally:
            if proc.returncode is None:
                log.debug('%s: terminating process: %r', name, proc)
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=3)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
                log.debug('%s: process terminated: %r', name, proc)
            await conn.close_metrics(name)


class SSHSession(Session):
    __slots__ = ('_stdout', '_ssh')

    def __init__(self, stdout: asyncssh.SSHReader[str], ssh: asyncssh.SSHClientConnection) -> None:
        super().__init__()
        self._stdout = stdout
        self._ssh = ssh

    def journal_lines(self) -> AsyncIterable[str]:
        return self._stdout

    def _connect_stream(self) -> Awaitable[StreamPair]:
        return self._ssh.open_connection('127.0.0.1', METRICS_PORT, encoding=None)

    async def run_command(self, cmd: str) -> str:
        res = await self._ssh.run(cmd, timeout=10)
        out = res.stdout or res.stderr
        if not out:
            return ''
        if isinstance(out, bytes):
            return out.decode('utf-8', errors='replace')
        return out


class SSHDialer(Dialer):
    __slots__ = ('host',)

    def __init__(self, host: str) -> None:
        self.host = host

    @asynccontextmanager
    async def open(self, name: str) -> AsyncGenerator[Session]:
        async with asyncssh.connect(
            self.host,
            config=SSH_CONFIG_FILES,
            login_timeout=15,
            options=asyncssh.SSHClientConnectionOptions(
                keepalive_interval=30,
                keepalive_count_max=3,
                tcp_keepalive=True,
                request_pty=False,
                utf8_decode_errors='replace',
                compression_algs='zlib@openssh.com',
                preferred_auth='publickey',
            ),
        ) as ssh:
            process = None
            conn = None
            try:
                process = await ssh.create_process(' '.join(JOURNAL_CMD))
                conn = SSHSession(process.stdout, ssh)
                yield conn
            finally:
                log.debug('%s: closing connection', name)
                if process is not None:
                    try:
                        process.terminate()
                    except OSError:
                        process = None
                if conn is not None:
                    await conn.close_metrics(name)
                if process is not None:
                    try:
                        await asyncio.wait_for(ssh.wait_closed(), timeout=10)
                    except asyncio.CancelledError:
                        log.debug('%s: connection cancelled: %s', name, self.host)
                        process.kill()
                        raise
                    except BaseException:
                        log.exception(
                            '%s: error while waiting for ssh to close: %s', name, self.host
                        )
                        process.kill()
                        raise
                log.debug('%s: connection closed: %s', name, self.host)
