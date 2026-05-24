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
    from collections.abc import AsyncGenerator, AsyncIterable


class StreamReaderProtocol(Protocol):
    async def read(self, n: int = -1) -> bytes: ...


class StreamWriterProtocol(Protocol):
    def write(self, data: bytes) -> None: ...
    async def drain(self) -> None: ...
    def close(self) -> None: ...
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
    'GET /metrics?' + '&'.join('collect%5B%5D=' + c for c in COLLECTORS) + ' HTTP/1.0\r\n\r\n'
).encode('ascii')


class Connection(ABC):
    __slots__ = ()

    @abstractmethod
    def journal_lines(self) -> AsyncIterable[str]: ...

    @abstractmethod
    async def _open_metrics(
        self,
    ) -> tuple[StreamReaderProtocol, StreamWriterProtocol]: ...

    async def fetch_metrics(self, name: str) -> str:
        reader, writer = await self._open_metrics()
        try:
            writer.write(REQUEST)
            await writer.drain()
            data = await reader.read()
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
        log.debug('%s: metrics: %d bytes', name, len(data))

        if log.isEnabledFor(logging.DEBUG):
            os.makedirs(TMP_DIR, exist_ok=True)
            with open(os.path.join(TMP_DIR, f'metrics-{name}.log'), 'wb') as fp:
                fp.write(data)

        head, sep, body = data.partition(b'\r\n\r\n')
        if not sep:
            raise RuntimeError(f'{name}: malformed HTTP response: {data[:80]!r}')
        status_line = head.split(b'\r\n', 1)[0]
        status = status_line.split(maxsplit=2)
        if len(status) < 2 or status[1] != b'200':
            raise RuntimeError(
                f'{name}: {status_line.decode('latin-1', 'replace').strip()}: '
                f'{body.decode('utf-8', 'replace').strip()[:200]}'
            )
        return body.decode('utf-8', errors='replace')


class Connecter(ABC):
    __slots__ = ()

    @abstractmethod
    def open(self, name: str) -> AbstractAsyncContextManager[Connection]: ...


class _LocalConnection(Connection):
    __slots__ = ('_stdout',)

    def __init__(self, stdout: asyncio.StreamReader) -> None:
        self._stdout = stdout

    async def journal_lines(self) -> AsyncIterable[str]:
        while line := await self._stdout.readline():
            yield line.decode('utf-8', errors='replace')

    async def _open_metrics(self) -> tuple[StreamReaderProtocol, StreamWriterProtocol]:
        return await asyncio.open_connection('127.0.0.1', METRICS_PORT)


class LocalConnecter(Connecter):
    __slots__ = ()

    @asynccontextmanager
    async def open(self, name: str) -> AsyncGenerator[Connection]:
        proc = await asyncio.create_subprocess_exec(
            *JOURNAL_CMD, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        try:
            yield _LocalConnection(proc.stdout)  # type: ignore[arg-type]
        finally:
            if proc.returncode is None:
                log.debug('%s: terminating process: %r', name, proc)
                proc.terminate()
                try:
                    # No need to handle CancelledError here, since it indicates the process
                    # already exited.
                    await asyncio.wait_for(proc.wait(), timeout=3)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
                log.debug('%s: process terminated: %r', name, proc)


class _SSHConnection(Connection):
    __slots__ = ('_stdout', '_ssh')

    def __init__(self, stdout: asyncssh.SSHReader[str], ssh: asyncssh.SSHClientConnection) -> None:
        self._stdout = stdout
        self._ssh = ssh

    def journal_lines(self) -> AsyncIterable[str]:
        return self._stdout

    async def _open_metrics(self) -> tuple[StreamReaderProtocol, StreamWriterProtocol]:
        return await self._ssh.open_connection('127.0.0.1', METRICS_PORT, encoding=None)


class SSHConnecter(Connecter):
    __slots__ = ('host',)

    def __init__(self, host: str) -> None:
        self.host = host

    @asynccontextmanager
    async def open(self, name: str) -> AsyncGenerator[Connection]:
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
            try:
                process = await ssh.create_process(' '.join(JOURNAL_CMD))
                yield _SSHConnection(process.stdout, ssh)
            finally:
                log.debug('%s: closing connection', name)
                if process is not None:
                    try:
                        process.terminate()
                    except OSError:
                        process = None
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
