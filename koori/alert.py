import html
import logging
import time

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from telegram.constants import ParseMode
from telegram.error import TelegramError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from telegram import Bot

    from .server import ServerState

log = logging.getLogger(__name__)

ALERT_RESEND = 600  # re-alert every 10 min

MEM_AVAIL_ALERT = 200 << 20  # 200 MiB
FS_AVAIL_ALERT = 2 << 30  # 2 GiB
TEMP_ALERT = 80


@dataclass(slots=True, eq=False, match_args=False)
class AlertState:
    keys: frozenset[str] = frozenset()
    sent_at: float = 0.0

    async def check(self, bot: Bot, chat_id: int, servers: Sequence[ServerState]) -> None:
        lines: list[str] = []
        keys: set[str] = set()
        now: float | None = None

        def push_time(ts: float | None = None) -> None:
            nonlocal now
            if ts is None:
                if now is None:
                    now = time.time()
                ts = now
            date = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            lines.append(f'⏱️ <tg-time unix="{int(ts)}" format="wdT">{date}</tg-time>')

        for s in servers:

            def recover(msg: str, symbol: str = '✅') -> None:
                lines.append(f'{symbol} <b>{html.escape(s.name)}</b>: {html.escape(msg)}')  # noqa: B023

            def alert(key: str, msg: str):
                keys.add(key)
                recover(msg, symbol='⚠️')

            down_key = f'{s.name}:down'
            if s.update is None:
                alert(down_key, f'down: {s.error or '<unknown error>'}')
                push_time()
            elif down_key in self.keys:
                recover('back online')
                push_time()

            m = s.metrics
            if m is None:
                continue

            old_lines = len(lines)

            mem_key = f'{s.name}:mem'
            mem_avail = m.mem_total - m.mem_used
            if m.mem_total > 0 and mem_avail <= MEM_AVAIL_ALERT:
                alert(mem_key, f'available memory low ({mem_avail >> 20} MiB)')
            elif mem_key in self.keys:
                recover(f'available memory recovered ({mem_avail >> 20} MiB)')

            fs_key = f'{s.name}:fs'
            fs_avail = m.fs_total - m.fs_used
            if m.fs_total > 0 and fs_avail <= FS_AVAIL_ALERT:
                alert(fs_key, f'available disk space low ({fs_avail / (1 << 30):.1f} GiB)')
            elif fs_key in self.keys:
                recover(f'available disk space recovered ({fs_avail / (1 << 30):.1f} GiB)')

            if m.temp is not None:
                temp_key = f'{s.name}:temp'
                if m.temp >= TEMP_ALERT:
                    alert(temp_key, f'{m.temp_chip or '<unknown sensor>'}: {m.temp:.1f}°C')
                elif temp_key in self.keys:
                    recover(f'{m.temp_chip or '<unknown sensor>'}: {m.temp:.1f}°C')

            sys_key = f'{s.name}:sys'
            if m.alerts:
                alert(sys_key, ' | '.join(m.alerts))
            elif sys_key in self.keys:
                recover('alerts cleared')

            if len(lines) != old_lines:
                push_time(m.timestamp)

        current = frozenset(keys)
        now = time.monotonic()
        changed = current != self.keys
        expired = now - self.sent_at >= ALERT_RESEND
        if not changed and (not current or not expired):
            return

        try:
            await bot.send_message(
                chat_id=chat_id,
                text='\n'.join(lines),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except TelegramError:
            log.exception('alert send failed')
        else:
            self.keys = current
            self.sent_at = now
