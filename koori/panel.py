import asyncio
import html
import logging
import re
import time

from datetime import datetime
from typing import TYPE_CHECKING

from telegram.constants import MessageLimit

if TYPE_CHECKING:
    from collections import deque
    from collections.abc import Sequence

    from .bot import PanelControl
    from .server import JournalEntry, ServerState

log = logging.getLogger(__name__)

TG_MESSAGE_LIMIT = MessageLimit.MAX_TEXT_LENGTH
LINE_TRIM = 180
BAR_WIDTH = 10
PARTIALS = '▏▎▍▌▋▊▉'
METRICS_FETCH_TIMEOUT = 10

_TAG_RE = re.compile(r'<[^>]+>')


def _eff_len(s: str) -> int:
    # Telegram's 4096 limit is post-parse: tags don't count, entities decode to one char.
    return len(html.unescape(_TAG_RE.sub('', s)))


def _fmt_size(x: float) -> str:
    for unit in ('B', 'K', 'M', 'G', 'T'):
        if x < 1024:
            return f'{x:.1f}{unit}'
        x /= 1024
    return f'{x:.1f}P'


def _fmt_bps(x: float | None) -> str:
    if x is None:
        return '  —  '
    for unit in ('B/s', 'K/s', 'M/s', 'G/s', 'T/s'):
        if x < 1024:
            return f'{x:5.1f}{unit}'
        x /= 1024
    return f'{x:5.1f}P/s'


def _fmt_duration(t: float) -> str:
    if t < 1e-3:
        return f'{t * 1e6:.1f}µs'
    if t < 1:
        return f'{t * 1e3:.1f}ms'
    return f'{t:.1f}s'


def _bar(pct: float | None) -> str:
    if pct is None:
        return '─' * BAR_WIDTH
    pct = max(0.0, min(100.0, pct))
    eighths = round(pct / 100 * BAR_WIDTH * 8)
    full, rem = divmod(eighths, 8)
    bar = '█' * full
    if rem and full < BAR_WIDTH:
        bar += PARTIALS[rem - 1]
        full += 1
    return bar + '░' * (BAR_WIDTH - full)


def _bar_dual(avg: float | None, peak: float | None) -> str:
    if avg is None:
        return '─' * BAR_WIDTH
    avg_c = max(0.0, min(100.0, avg))
    peak_c = max(avg_c, min(100.0, peak)) if peak is not None else avg_c

    eighths = round(avg_c / 100 * BAR_WIDTH * 8)
    full, rem = divmod(eighths, 8)
    bar = '█' * full
    used = full
    if rem and full < BAR_WIDTH:
        bar += PARTIALS[rem - 1]
        used += 1

    peak_chars = round(peak_c / 100 * BAR_WIDTH)
    if peak_chars > used:
        bar += '▒' * (peak_chars - used)
        used = peak_chars

    return bar + '░' * (BAR_WIDTH - used)


def _pct_str(pct: float | None) -> str:
    return f'{pct:4.1f}%' if pct is not None else '  —  '


def _gauge(label: str, pct: float | None, used: int = 0, total: int = 0) -> str:
    return f'{label:<3} {_bar(pct)} {_pct_str(pct)}' + (
        f' {_fmt_size(used):>6} / {_fmt_size(total):>6}' if total > 0 else ''
    )


def _cpu_row(avg: float | None, peak: float | None, count: int) -> str:
    cores = f' / {f'{count}x':>6}' if count > 0 else ''
    peak_s = f' {_pct_str(peak).strip():>6}' if peak is not None else ''
    return f'CPU {_bar_dual(avg, peak)} {_pct_str(avg)}{peak_s}{cores}'


def _trim(line: str, n: int = LINE_TRIM) -> str:
    return line if len(line) <= n else line[: n - 1] + '…'


async def _render_server(server: ServerState, skip_update: bool) -> Sequence[str]:
    name = f'<b>{html.escape(server.name)}</b>'
    if (func := server.update) is None:
        header = '🔴 ' + name
        return (header, f'offline: {html.escape(server.error or '<unknown>')}')

    if skip_update and (m := server.metrics) is not None:
        dt = ''
    else:
        t0 = time.monotonic()
        try:
            m = await asyncio.wait_for(func(), timeout=METRICS_FETCH_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception('%s: error fetching metrics', server.name)
            dt = time.monotonic() - t0
            header = '🟡 ' + name
            err = f'error: {html.escape(str(e))}'
            if (m := server.metrics) is None:
                return (header, err)
            header += f'\n{err}'
        else:
            dt = time.monotonic() - t0
        dt = _fmt_duration(dt) + ', '

    date = datetime.fromtimestamp(m.timestamp).strftime('%H:%M:%S').lstrip('0')
    header = (
        f'🟢 {name}  ({dt}{_fmt_duration(m.scrape_time)}, '
        f'<tg-time unix="{int(m.timestamp)}" format="r">{date}</tg-time>)'
    )

    pct_mem = 100 * m.mem_used / m.mem_total if m.mem_total > 0 else None
    pct_swap = 100 * m.swap_used / m.swap_total if m.swap_total > 0 else None
    pct_fs = 100 * m.fs_used / m.fs_total if m.fs_total > 0 else None

    rows = [
        _cpu_row(m.cpu_pct, m.cpu_max_pct, m.cpu_count),
        _gauge('MEM', pct_mem, m.mem_used, m.mem_total),
    ]
    if m.swap_total > 0:
        rows.append(_gauge('SWP', pct_swap, m.swap_used, m.swap_total))
    if m.fs_mount is not None:
        rows.append(_gauge(m.fs_mount, pct_fs, m.fs_used, m.fs_total))
    rows.append(
        f'NET ↓{_fmt_bps(m.rx_bps)} ({_fmt_size(m.rx)}) ↑{_fmt_bps(m.tx_bps)} ({_fmt_size(m.tx)})'
    )

    days, rem = divmod(int(m.timestamp - m.boot_time), 86400)
    hours, rem = divmod(rem, 3600)
    parts = [
        days and f'{days}d',
        hours and f'{hours}h',
        not days and rem >= 60 and f'{rem // 60}m',
    ]
    if not parts:
        parts.append(f'{rem}s')

    ext = [f'Load: {m.load1:.2f} | Up: ', ' '.join(p for p in parts if p)]
    if m.temp:
        ext.append(f' | {m.temp:.1f}°C')
    rows.append(''.join(ext))

    if m.alerts:
        rows.append('⚠️ ' + ' | '.join(m.alerts))

    return (header, f'<pre>{html.escape('\n'.join(rows))}</pre>')


def _fit_journal(
    journal: Sequence[JournalEntry], budget: int, *, expanded: bool
) -> tuple[str, int, int] | None:
    '''Fit as many entries as possible into budget eff-chars, newest-first.

    Default mode wraps in <blockquote expandable>…</blockquote> — visually collapsed by Telegram.
    Contents stay newest-first. Expanded mode reverses to chronological asc inside a plain
    <pre>, journalctl-style.
    '''
    if budget <= 0:
        return None

    rows = []
    eff = 0
    for entry in reversed(journal):
        new_eff = eff + len(s := _trim(entry.message)) + bool(rows)
        if new_eff > budget:
            break
        eff = new_eff
        rows.append(s)

    if rows:
        if expanded:
            return f'<pre>{html.escape('\n'.join(reversed(rows)))}</pre>', eff, len(rows)
        return f'<blockquote expandable>{html.escape('\n'.join(rows))}</blockquote>', eff, len(rows)


async def render_panel(
    servers: Sequence[ServerState], journal: deque[JournalEntry], ctrl: PanelControl
) -> str:
    server_blocks = await asyncio.gather(*(_render_server(s, ctrl.skip_update) for s in servers))
    ctrl.skip_update = False

    text = '\n'.join('\n'.join(b) for b in server_blocks)
    eff = _eff_len(text)
    while eff > TG_MESSAGE_LIMIT and len(server_blocks) > 1:
        server_blocks.pop()
        text = '\n'.join('\n'.join(b) for b in server_blocks)
        eff = _eff_len(text)
    if eff > TG_MESSAGE_LIMIT:
        return f'<i>panel too large to render ({eff} chars)</i>'

    # -1 for the '\n' separator between server section and journal section.
    journal_section = _fit_journal(journal, TG_MESSAGE_LIMIT - eff - 1, expanded=ctrl.expanded)
    if journal_section is not None:
        journal_text, journal_eff, journal_cnt = journal_section
        eff += journal_eff + 1
        text += '\n' + journal_text
        log.debug(
            'render_panel: journal=%d/%d, len=%d, eff=%d (%d)',
            journal_cnt,
            len(journal),
            len(text),
            eff,
            journal_eff,
        )
    else:
        log.debug('render_panel: journal=%d, len=%d, eff=%d', len(journal), len(text), eff)
    return text
