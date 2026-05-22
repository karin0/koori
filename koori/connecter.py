import asyncio
import contextlib
import logging
import os
import tempfile

from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING

import asyncssh

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

UDS_DIR = os.path.join(tempfile.gettempdir(), 'koori')

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

JOURNAL_CMD = ('journalctl', '-f', '-n', '15', '-o', 'short', '--no-pager')
METRICS_PORT = 9100


class Connection(ABC):
    __slots__ = ()

    @abstractmethod
    def journal_lines(self) -> AsyncIterator[str]: ...

    @abstractmethod
    async def _open_metrics(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]: ...

    async def fetch_metrics(self) -> str:
        reader, writer = await self._open_metrics()
        try:
            writer.write(b'GET /metrics HTTP/1.0\r\n\r\n')
            await writer.drain()
            data = await reader.read()
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
        head, sep, body = data.partition(b'\r\n\r\n')
        if not sep:
            raise RuntimeError(f'malformed HTTP response: {data[:80]!r}')
        status_line = head.split(b'\r\n', 1)[0]
        status = status_line.split(maxsplit=2)
        if len(status) < 2 or status[1] != b'200':
            raise RuntimeError(f'HTTP {status_line.decode("latin-1", "replace")!r}')
        return body.decode('utf-8', errors='replace')


class Connecter(ABC):
    __slots__ = ()

    @abstractmethod
    def open(self) -> AbstractAsyncContextManager[Connection]: ...


class _LocalConnection(Connection):
    __slots__ = ('_stdout',)

    def __init__(self, stdout: asyncio.StreamReader) -> None:
        self._stdout = stdout

    async def journal_lines(self) -> AsyncIterator[str]:
        while True:
            line = await self._stdout.readline()
            if not line:
                break
            yield line.decode('utf-8', errors='replace').rstrip()

    async def _open_metrics(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection('127.0.0.1', METRICS_PORT)


class LocalConnecter(Connecter):
    __slots__ = ()

    @asynccontextmanager
    async def open(self) -> AsyncGenerator[Connection]:
        proc = await asyncio.create_subprocess_exec(
            *JOURNAL_CMD, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        try:
            yield _LocalConnection(proc.stdout)  # type: ignore[arg-type]
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=3)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()


class _SSHConnection(Connection):
    __slots__ = ('_stdout', '_uds_path')

    def __init__(self, stdout: asyncssh.SSHReader[str], uds_path: str) -> None:
        self._stdout = stdout
        self._uds_path = uds_path

    async def journal_lines(self) -> AsyncIterator[str]:
        async for line in self._stdout:
            yield line.rstrip()

    async def _open_metrics(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_unix_connection(self._uds_path)


class SSHConnecter(Connecter):
    __slots__ = ('host',)

    def __init__(self, host: str) -> None:
        self.host = host

    @asynccontextmanager
    async def open(self) -> AsyncGenerator[Connection]:
        os.makedirs(UDS_DIR, exist_ok=True)
        uds_path = os.path.join(UDS_DIR, f'{self.host}.sock')
        with contextlib.suppress(FileNotFoundError):
            os.unlink(uds_path)

        async with asyncssh.connect(self.host, config=SSH_CONFIG_FILES) as ssh:
            listener = None
            process = None
            try:
                listener = await ssh.forward_local_path_to_port(uds_path, '127.0.0.1', METRICS_PORT)
                process = await ssh.create_process(' '.join(JOURNAL_CMD))
                yield _SSHConnection(process.stdout, uds_path)
            finally:
                log.debug('closing connection: %s', self.host)
                if process is not None:
                    try:
                        process.terminate()
                    except OSError:
                        log.exception('failed to terminate: %s', self.host)
                if listener is not None:
                    listener.close()
                try:
                    os.unlink(uds_path)
                except FileNotFoundError:
                    pass
                except OSError:
                    log.exception('failed to unlink UDS: %s', uds_path)
                log.debug('connection closed: %s', self.host)
