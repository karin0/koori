import asyncio
import contextlib
import logging
import os
import tempfile

from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING

import aiohttp
import asyncssh

from .metric import COLLECTORS

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterable

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


class Connection(ABC):
    __slots__ = ('_session', '_url')

    def __init__(self, session: aiohttp.ClientSession, url: str) -> None:
        self._session = session
        self._url = url

    @abstractmethod
    def journal_lines(self) -> AsyncIterable[str]: ...

    async def fetch_metrics(self, name: str) -> str:
        params = [('collect[]', c) for c in COLLECTORS]
        async with self._session.get(self._url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.text(errors='replace')

        log.debug('%s: metrics: %d bytes', name, len(data))

        if log.isEnabledFor(logging.DEBUG):
            os.makedirs(TMP_DIR, exist_ok=True)
            with open(os.path.join(TMP_DIR, f'metrics-{name}.log'), 'w', encoding='utf-8') as fp:
                fp.write(data)

        return data


class Connecter(ABC):
    __slots__ = ()

    @abstractmethod
    def open(self, name: str) -> AbstractAsyncContextManager[Connection]: ...


class _LocalConnection(Connection):
    __slots__ = ('_stdout',)

    def __init__(self, stdout: asyncio.StreamReader, session: aiohttp.ClientSession) -> None:
        super().__init__(session, f'http://127.0.0.1:{METRICS_PORT}/metrics')
        self._stdout = stdout

    async def journal_lines(self) -> AsyncIterable[str]:
        while line := await self._stdout.readline():
            yield line.decode('utf-8', errors='replace')


class LocalConnecter(Connecter):
    __slots__ = ()

    @asynccontextmanager
    async def open(self, name: str) -> AsyncGenerator[Connection]:
        proc = await asyncio.create_subprocess_exec(
            *JOURNAL_CMD, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        async with aiohttp.ClientSession() as session:
            try:
                yield _LocalConnection(proc.stdout, session)  # type: ignore[arg-type]
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


class _SSHConnection(Connection):
    __slots__ = ('_stdout',)

    def __init__(self, stdout: asyncssh.SSHReader[str], session: aiohttp.ClientSession) -> None:
        super().__init__(session, 'http://localhost/metrics')
        self._stdout = stdout

    def journal_lines(self) -> AsyncIterable[str]:
        return self._stdout


class SSHConnecter(Connecter):
    __slots__ = ('host',)

    def __init__(self, host: str) -> None:
        self.host = host

    @asynccontextmanager
    async def open(self, name: str) -> AsyncGenerator[Connection]:
        os.makedirs(TMP_DIR, exist_ok=True)
        uds_path = os.path.join(TMP_DIR, f'{name}.sock')
        with contextlib.suppress(FileNotFoundError):
            os.unlink(uds_path)

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
            listener = None
            process = None
            try:
                listener = await ssh.forward_local_path_to_port(uds_path, '127.0.0.1', METRICS_PORT)
                process = await ssh.create_process(' '.join(JOURNAL_CMD))
                connector = aiohttp.UnixConnector(path=uds_path)
                async with aiohttp.ClientSession(connector=connector) as session:
                    yield _SSHConnection(process.stdout, session)
            finally:
                log.debug('%s: closing connection: %s', name, uds_path)
                if process is not None:
                    try:
                        process.terminate()
                    except OSError:
                        process = None
                if listener is not None:
                    listener.close()
                try:
                    os.unlink(uds_path)
                except FileNotFoundError:
                    pass
                except OSError:
                    log.exception('%s: failed to unlink UDS: %s', name, uds_path)
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
