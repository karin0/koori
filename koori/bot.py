import asyncio
import contextlib
import json
import logging
import os
import time

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from aiohttp import ClientTimeout
from ptbcontrib.aiohttp_request import AiohttpRequest
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter, TelegramError

from .alert import AlertState
from .panel import render_panel

if TYPE_CHECKING:
    from collections import deque
    from collections.abc import Iterable, Sequence

    from .server import JournalEntry, ServerState

log = logging.getLogger(__name__)

STATE_FILE = Path('state.json')

DEFAULT_INTERVAL = 5.0
MIN_INTERVAL = 1.0
MAX_INTERVAL = 60.0

HTTP_TIMEOUT = 30
HTTP_GET_UPDATES_TIMEOUT = 40


@dataclass(slots=True, eq=False, match_args=False)
class PanelControl:
    interval: float = DEFAULT_INTERVAL
    expanded: bool = False
    refresh: asyncio.Event = field(default_factory=asyncio.Event)
    skip_update: bool = False


def _keyboard(ctrl: PanelControl) -> InlineKeyboardMarkup:
    now = datetime.now()
    log_icon = '📃' if ctrl.expanded else '📜'
    return InlineKeyboardMarkup(
        (
            (
                InlineKeyboardButton('🐢', callback_data='slower'),
                InlineKeyboardButton(
                    f'🔄{now.strftime('%H:%M:%S').lstrip('0')}', callback_data='refresh'
                ),
                InlineKeyboardButton('🐇', callback_data='faster'),
                InlineKeyboardButton(f'{log_icon}{ctrl.interval:g}s', callback_data='log'),
            ),
        )
    )


def build_bot() -> Bot:
    if local_server := os.environ.get('LOCAL_SERVER'):
        base = local_server.rstrip('/')
        args = (base + '/bot', base + '/file/bot')
    else:
        args = ()

    return Bot(
        os.environ['TELEGRAM_BOT_TOKEN'],
        *args,
        request=AiohttpRequest(
            connection_pool_size=4, client_timeout=ClientTimeout(total=HTTP_TIMEOUT)
        ),
        get_updates_request=AiohttpRequest(
            connection_pool_size=1, client_timeout=ClientTimeout(total=HTTP_GET_UPDATES_TIMEOUT)
        ),
    )


async def ensure_panel_message(
    bot: Bot, chat_id: int, ctrl: PanelControl, *, text: str | None = None
) -> int:
    key = str(chat_id)
    try:
        with STATE_FILE.open(encoding='utf-8') as fp:
            state = json.load(fp)
    except FileNotFoundError:
        state = {}
    else:
        if text is None and (val := state.get(key)) is not None:
            return val

    msg = await bot.send_message(
        chat_id=chat_id,
        text=text or 'starting…',
        reply_markup=_keyboard(ctrl),
        disable_web_page_preview=True,
    )
    try:
        await msg.pin()
    except TelegramError as e:
        log.warning('pin failed (continuing anyway): %s', e)

    state[key] = val = msg.message_id
    with STATE_FILE.open('w', encoding='utf-8') as fp:
        json.dump(state, fp, ensure_ascii=False)
    return val


async def wait_servers(sema: asyncio.Semaphore, iter: Iterable) -> None:
    for _ in iter:
        await sema.acquire()


async def render_loop(
    bot: Bot,
    chat_id: int,
    alert_chat_id: int,
    states: Sequence[ServerState],
    journal: deque[JournalEntry],
    ctrl: PanelControl,
    sema: asyncio.Semaphore,
) -> None:
    msg_id = await ensure_panel_message(bot, chat_id, ctrl)
    try:
        await asyncio.wait_for(wait_servers(sema, states), timeout=ctrl.interval)
    except TimeoutError:
        info = (
            'timed out waiting for servers up; offline: '
            f'{', '.join(s.name for s in states if s.update is None)}'
        )
        log.warning('%s', info)
    else:
        info = f'all servers up: {', '.join(s.name for s in states)}'
        log.info('%s', info)

    await bot.send_message(chat_id=alert_chat_id, text=info, disable_web_page_preview=True)
    del sema, info
    alert = AlertState()
    scheduled = None

    while True:
        text, _ = await asyncio.gather(
            render_panel(states, journal, ctrl), alert.check(bot, alert_chat_id, states, journal)
        )
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=_keyboard(ctrl),
                disable_web_page_preview=True,
            )
        except RetryAfter as e:
            wait = e.retry_after
            if isinstance(wait, timedelta):
                wait = wait.total_seconds()
            log.warning('rate-limited: %r, sleeping %ss', e, wait)
            await asyncio.sleep(wait)
            continue
        except BadRequest as e:
            if 'not modified' not in str(e).lower():
                log.error('edit failed: %s', e)
                msg_id = await ensure_panel_message(bot, chat_id, ctrl, text=text)
        except TelegramError:
            log.exception('edit failed')

        if scheduled:
            timeout = max(scheduled - time.monotonic(), 1)
            scheduled = None
        else:
            t0 = time.monotonic()
            timeout = ctrl.interval

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(ctrl.refresh.wait(), timeout=timeout)

        if ctrl.refresh.is_set():
            ctrl.refresh.clear()
            if ctrl.skip_update:
                scheduled = t0 + ctrl.interval  # pyright: ignore[reportPossiblyUnboundVariable]


async def update_poller(bot: Bot, chat_id: int, ctrl: PanelControl) -> None:
    # Skip any pending updates from before this session.
    pending = await bot.get_updates(offset=-1, limit=1, timeout=0)
    offset = (pending[-1].update_id + 1) if pending else 0

    while True:
        try:
            updates = await bot.get_updates(
                offset=offset, timeout=HTTP_TIMEOUT, allowed_updates=['callback_query']
            )
        except TelegramError:
            log.exception('get_updates failed')
            await asyncio.sleep(2)
            continue
        for update in updates:
            offset = update.update_id + 1
            cb = update.callback_query
            if cb is None or cb.message is None or cb.message.chat.id != chat_id:
                continue
            match cb.data:
                case 'slower':
                    ctrl.interval = min(MAX_INTERVAL, ctrl.interval * 2)
                case 'faster':
                    ctrl.interval = max(MIN_INTERVAL, ctrl.interval / 2)
                case 'log':
                    ctrl.expanded = not ctrl.expanded
                    ctrl.skip_update = True
                case _:
                    pass
            log.debug('ctrl: interval=%g expanded=%s', ctrl.interval, ctrl.expanded)
            ctrl.refresh.set()
            await cb.answer()
