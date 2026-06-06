import asyncio
import html
import logging
import time

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import asyncssh

from telegram.constants import ParseMode
from telegram.error import TelegramError

from .panel import TG_MESSAGE_LIMIT, _fit_journal

if TYPE_CHECKING:
    from collections.abc import Sequence

    from telegram import Bot

    from .server import JournalEntry, ServerState

log = logging.getLogger(__name__)

ALERT_RESEND = 600  # re-alert every 10 min

MEM_AVAIL_ALERT = 200 << 20  # 200 MiB
FS_AVAIL_ALERT = 2 << 30  # 2 GiB
TEMP_ALERT = 80


@dataclass(slots=True, eq=False, match_args=False)
class AlertState:
    keys: frozenset[str] = frozenset()

    async def _check_server(
        self, s: ServerState, journal: Sequence[JournalEntry], now: float, now_ts: float
    ) -> tuple[ServerState, set[str], str]:
        s_keys: set[str] = set()
        server_lines: list[str] = []
        eff = 0
        changed = False

        def push_time(ts: float | None = None) -> None:
            nonlocal eff
            if ts is None:
                ts = now_ts
            date = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            server_lines.append(f'⏱️ <tg-time unix="{int(ts)}" format="wdT">{date}</tg-time>')
            eff += len(date) + 2

        def alert(msg: str, *, symbol: str = '⚠️', key: str | None = None) -> None:
            nonlocal eff, changed
            if key is None:
                changed = True
            else:
                s_keys.add(key)
                if not changed:
                    changed = key not in self.keys
            server_lines.append(f'{symbol} <b>{html.escape(s.name)}</b>: {html.escape(msg)}')
            eff += len(s.name) + len(msg) + len(symbol) + 3

        def recover(msg: str) -> None:
            alert(msg, symbol='✅')

        # Check down state
        down_key = f'{s.name}:down'
        if s.update is None:
            alert(f'down: {s.error or '<unknown error>'}', key=down_key)
            push_time()
        elif down_key in self.keys:
            recover('back online')
            push_time()

        if (m := s.metrics) is not None:
            old_lines = len(server_lines)

            mem_key = f'{s.name}:mem'
            mem_avail = m.mem_total - m.mem_used
            if m.mem_total > 0 and mem_avail <= MEM_AVAIL_ALERT:
                alert(f'available memory low ({mem_avail >> 20} MiB)', key=mem_key)
            elif mem_key in self.keys:
                recover(f'available memory recovered ({mem_avail >> 20} MiB)')

            fs_key = f'{s.name}:fs'
            fs_avail = m.fs_total - m.fs_used
            if m.fs_total > 0 and fs_avail <= FS_AVAIL_ALERT:
                alert(f'available disk space low ({fs_avail / (1 << 30):.1f} GiB)', key=fs_key)
            elif fs_key in self.keys:
                recover(f'available disk space recovered ({fs_avail / (1 << 30):.1f} GiB)')

            if m.temp is not None:
                temp_key = f'{s.name}:temp'
                if m.temp >= TEMP_ALERT:
                    alert(f'{m.temp_chip or '<unknown sensor>'}: {m.temp:.1f}°C', key=temp_key)
                elif temp_key in self.keys:
                    recover(f'{m.temp_chip or '<unknown sensor>'}: {m.temp:.1f}°C')

            sys_key = f'{s.name}:sys'
            if m.alerts:
                alert(' | '.join(m.alerts), key=sys_key)
            elif sys_key in self.keys:
                recover('alerts cleared')

            if m.cnt_alerts:
                alert(' | '.join(m.cnt_alerts))

            if len(server_lines) != old_lines:
                push_time(m.timestamp)

        if not (changed or (s_keys and (now - s.alert_at >= ALERT_RESEND))):
            return s, s_keys, ''

        if s.session is not None:
            try:
                proc_info = await s.session.run_command('ps -eo pcpu,pmem,args --sort=-pcpu')
            except (OSError, TimeoutError, asyncssh.Error) as e:
                log.warning('%s: failed to fetch busiest processes: %s', s.name, e)
            else:
                lines_proc = proc_info.splitlines()[:10]
                proc_info = '\n'.join(lines_proc)
                eff += len(proc_info)
                if proc_info:
                    server_lines.append(
                        f'<blockquote expandable><pre>{html.escape(proc_info)}</pre></blockquote>'
                    )

        if (
            (server_logs := tuple(entry for entry in journal if entry.server is s))
            and (budget := TG_MESSAGE_LIMIT - eff) > 0
            and (fit := _fit_journal(server_logs, budget, expanded=False)) is not None
        ):
            server_lines.append(fit[0])

        return s, s_keys, '\n'.join(server_lines)

    async def check(
        self,
        bot: Bot,
        chat_id: int,
        servers: Sequence[ServerState],
        journal: Sequence[JournalEntry],
    ) -> None:
        now = time.monotonic()
        ts = time.time()

        new_keys = set()
        for s, s_keys, text in await asyncio.gather(
            *(self._check_server(s, journal, now, ts) for s in servers)
        ):
            if text:
                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                except TelegramError:
                    log.exception('alert send failed: %s', text)
                    continue
                s.alert_at = now
            new_keys.update(s_keys)
        self.keys = frozenset(new_keys)
