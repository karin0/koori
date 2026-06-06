import asyncio
import logging

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, NamedTuple

from .metric import MetricsSnapshot, parse_metrics

if TYPE_CHECKING:
    from collections import deque
    from collections.abc import Awaitable, Callable

    from .dialer import Dialer, Session

log = logging.getLogger(__name__)

RECONNECT_BACKOFF_MAX = 60


@dataclass(slots=True, eq=False, match_args=False)
class ServerState:
    name: str
    dialer: Dialer
    error: str | None = None
    metrics: MetricsSnapshot | None = None
    exclude: list[str] = field(default_factory=list)
    session: Session | None = None
    alert_at: float = 0
    alert_keys: frozenset[str] = field(default_factory=frozenset)

    async def _update_metrics(self, session: Session) -> MetricsSnapshot:
        self.metrics = metrics = parse_metrics(self, await session.fetch_metrics(self.name))
        return metrics

    @property
    def update(self) -> Callable[[], Awaitable[MetricsSnapshot]] | None:
        if (session := self.session) is not None:
            return lambda: self._update_metrics(session)

    async def run(
        self, journal: deque[JournalEntry], sema: asyncio.Semaphore | None = None
    ) -> None:
        backoff = 1
        while True:
            try:
                async with self.dialer.open(self.name) as session:
                    self.session = session
                    self.error = None
                    backoff = 1
                    if sema is not None:
                        sema.release()
                        sema = None
                        log.debug('%s: connected', self.name)
                    else:
                        log.info('%s: reconnected', self.name)

                    async for line in session.journal_lines():
                        if (line := line.strip()) and not any(kw in line for kw in self.exclude):
                            journal.append(JournalEntry(self, line))

            except asyncio.CancelledError:
                self.session = None
                self.error = 'cancelled'
                raise
            except Exception as e:
                self.session = None
                self.error = f'{type(e).__name__}: {e}'
                log.exception('%s: %s', self.name, self.error)
            else:
                self.session = None
                self.error = 'journal stream ended'
                log.warning('%s: %s', self.name, self.error)
            await asyncio.sleep(backoff)
            backoff = min(backoff << 1, RECONNECT_BACKOFF_MAX)


class JournalEntry(NamedTuple):
    server: ServerState
    message: str
