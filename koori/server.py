import asyncio
import logging

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .metric import MetricsSnapshot, parse_metrics

if TYPE_CHECKING:
    from collections import deque
    from collections.abc import Awaitable, Callable

    from .connecter import Connecter, Connection

log = logging.getLogger(__name__)

RECONNECT_BACKOFF_MAX = 60

type JournalEntry = str


@dataclass(slots=True, eq=False, match_args=False)
class ServerState:
    name: str
    connecter: Connecter
    error: str | None = None
    metrics: MetricsSnapshot | None = None
    update: Callable[[], Awaitable[MetricsSnapshot]] | None = None
    exclude: list[str] = field(default_factory=list)

    # cpu_id -> (idle, total) seconds, for per-core rate computation
    _prev_cpu: dict[str, tuple[float, float]] = field(default_factory=dict)
    _prev_net: tuple[float, float, float] | None = None  # (ts, rx, tx)

    async def _update_metrics(self, conn: Connection) -> MetricsSnapshot:
        self.metrics = metrics = parse_metrics(self, await conn.fetch_metrics(self.name))
        return metrics


async def _journal_loop(conn: Connection, state: ServerState, journal: deque[JournalEntry]) -> None:
    async for line in conn.journal_lines():
        if (line := line.strip()) and not any(kw in line for kw in state.exclude):
            journal.append(line)


async def run_server(
    server: ServerState, journal: deque[JournalEntry], sema: asyncio.Semaphore | None = None
) -> None:
    backoff = 1
    while True:
        try:
            async with server.connecter.open() as conn:
                server.update = lambda: server._update_metrics(conn)
                server.error = None
                backoff = 1
                log.info('%s: connected', server.name)
                if sema is not None:
                    sema.release()
                    sema = None
                await _journal_loop(conn, server, journal)
        except asyncio.CancelledError:
            server.update = None
            server.error = 'cancelled'
            raise
        except Exception as e:
            server.update = None
            server.error = f'{type(e).__name__}: {e}'
            log.exception('%s: %s', server.name, server.error)
        else:
            server.update = None
            server.error = 'journal stream ended'
            log.warning('%s: %s', server.name, server.error)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX)
