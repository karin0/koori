import asyncio
import logging

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .metric import MetricsSnapshot, parse_metrics

if TYPE_CHECKING:
    from collections import deque
    from collections.abc import Awaitable, Callable

    from .dialer import Dialer, Session

log = logging.getLogger(__name__)

RECONNECT_BACKOFF_MAX = 60

type JournalEntry = str


@dataclass(slots=True, eq=False, match_args=False)
class ServerState:
    name: str
    dialer: Dialer
    error: str | None = None
    metrics: MetricsSnapshot | None = None
    update: Callable[[], Awaitable[MetricsSnapshot]] | None = None
    exclude: list[str] = field(default_factory=list)

    # cpu_id -> (idle, total) seconds, for per-core rate computation
    _prev_cpu: dict[str, tuple[float, float]] = field(default_factory=dict)
    _prev_net: tuple[float, float, float] | None = None  # (ts, rx, tx)

    async def _update_metrics(self, conn: Session) -> MetricsSnapshot:
        self.metrics = metrics = parse_metrics(self, await conn.fetch_metrics(self.name))
        return metrics

    async def run(
        self, journal: deque[JournalEntry], sema: asyncio.Semaphore | None = None
    ) -> None:
        backoff = 1
        while True:
            try:
                async with self.dialer.open(self.name) as conn:
                    self.update = lambda: self._update_metrics(conn)
                    self.error = None
                    backoff = 1
                    log.info('%s: connected', self.name)
                    if sema is not None:
                        sema.release()
                        sema = None

                    async for line in conn.journal_lines():
                        if (line := line.strip()) and not any(kw in line for kw in self.exclude):
                            journal.append(line)

            except asyncio.CancelledError:
                self.update = None
                self.error = 'cancelled'
                raise
            except Exception as e:
                self.update = None
                self.error = f'{type(e).__name__}: {e}'
                log.exception('%s: %s', self.name, self.error)
            else:
                self.update = None
                self.error = 'journal stream ended'
                log.warning('%s: %s', self.name, self.error)
            await asyncio.sleep(backoff)
            backoff = min(backoff << 1, RECONNECT_BACKOFF_MAX)
