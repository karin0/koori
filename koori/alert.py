import asyncio
import html
import logging
import time

from datetime import datetime
from typing import TYPE_CHECKING

import asyncssh

from telegram.constants import ParseMode
from telegram.error import TelegramError

from .panel import TG_MESSAGE_LIMIT, _fit_journal

if TYPE_CHECKING:
    from collections.abc import Awaitable, Sequence

    from telegram import Bot

    from .server import JournalEntry, ServerState

log = logging.getLogger(__name__)

ALERT_RESEND = 600  # re-alert every 10 min

MEM_AVAIL_ALERT = 200 << 20  # 200 MiB
FS_AVAIL_ALERT = 2 << 30  # 2 GiB
TEMP_ALERT = 80


async def _check_server(
    bot: Bot,
    chat_id: int,
    s: ServerState,
    journal: Sequence[JournalEntry],
    now: float,
    now_ts: float,
):
    keys = s.alert_keys
    new_keys: set[str] = set()
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
            new_keys.add(key)
            if not changed:
                changed = key not in keys
        server_lines.append(f'{symbol} <b>{html.escape(s.name)}</b>: {html.escape(msg)}')
        eff += len(s.name) + len(msg) + len(symbol) + 3

    def recover(msg: str) -> None:
        alert(msg, symbol='✅')

    # Check down state
    if s.update is None:
        alert(f'down: {s.error or '<unknown error>'}', key='down')
        push_time()
    elif 'down' in keys:
        recover('back online')
        push_time()

    if (m := s.metrics) is not None:
        old_lines = len(server_lines)

        mem_avail = m.mem_total - m.mem_used
        if m.mem_total > 0 and mem_avail <= MEM_AVAIL_ALERT:
            alert(f'available memory low ({mem_avail >> 20} MiB)', key='mem')
        elif 'mem' in keys:
            recover(f'available memory recovered ({mem_avail >> 20} MiB)')

        fs_avail = m.fs_total - m.fs_used
        if m.fs_total > 0 and fs_avail <= FS_AVAIL_ALERT:
            alert(f'available disk space low ({fs_avail / (1 << 30):.1f} GiB)', key='fs')
        elif 'fs' in keys:
            recover(f'available disk space recovered ({fs_avail / (1 << 30):.1f} GiB)')

        if m.temp is not None:
            if m.temp >= TEMP_ALERT:
                alert(f'{m.temp_chip or '<unknown sensor>'}: {m.temp:.1f}°C', key='temp')
            elif 'temp' in keys:
                recover(f'{m.temp_chip or '<unknown sensor>'}: {m.temp:.1f}°C')

        if m.alerts:
            alert(' | '.join(m.alerts), key='sys')
        elif 'sys' in keys:
            recover('alerts cleared')

        if m.cnt_alerts:
            alert(' | '.join(m.cnt_alerts))

        if len(server_lines) != old_lines:
            push_time(m.timestamp)

    s.alert_keys = frozenset(new_keys)
    if not (changed or (new_keys and (now - s.alert_at >= ALERT_RESEND))):
        return

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

    text = '\n'.join(server_lines)
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except TelegramError:
        log.exception('alert send failed: %s', text)
    else:
        s.alert_at = now


def check_alert(
    bot: Bot, chat_id: int, servers: Sequence[ServerState], journal: Sequence[JournalEntry]
) -> Awaitable:
    now = time.monotonic()
    ts = time.time()
    return asyncio.gather(*(_check_server(bot, chat_id, s, journal, now, ts) for s in servers))
