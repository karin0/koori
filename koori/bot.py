import asyncio
import contextlib
import json
import logging
import os

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from aiohttp import ClientTimeout
from ptbcontrib.aiohttp_request import AiohttpRequest
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter, TelegramError

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
LONG_POLL_TIMEOUT = 30  # seconds Telegram holds the request open
HTTP_TIMEOUT = 30
HTTP_GET_UPDATES_TIMEOUT = LONG_POLL_TIMEOUT + 10  # HTTP timeout must exceed long-poll


@dataclass
class PanelControl:
    interval: float = DEFAULT_INTERVAL
    expanded: bool = False
    refresh: asyncio.Event = field(default_factory=asyncio.Event)


def _keyboard(ctrl: PanelControl) -> InlineKeyboardMarkup:
    now = datetime.now()
    log_icon = '📃' if ctrl.expanded else '📜'
    return InlineKeyboardMarkup(
        (
            (
                InlineKeyboardButton('🐢', callback_data='slower'),
                InlineKeyboardButton(f'↻ {now.strftime("%H:%M:%S")}', callback_data='refresh'),
                InlineKeyboardButton('🐇', callback_data='faster'),
                InlineKeyboardButton(f'{log_icon} {ctrl.interval:g}s', callback_data='log'),
            ),
        )
    )


def build_bot() -> Bot:
    token = os.environ['TELEGRAM_BOT_TOKEN']

    if local_server := os.environ.get('LOCAL_SERVER'):
        base = local_server.rstrip('/')
        args = (base + '/bot', base + '/file/bot')
    else:
        args = ()

    return Bot(
        token,
        *args,
        request=AiohttpRequest(
            connection_pool_size=4,
            client_timeout=ClientTimeout(total=HTTP_TIMEOUT),
        ),
        get_updates_request=AiohttpRequest(
            connection_pool_size=1,
            client_timeout=ClientTimeout(total=HTTP_GET_UPDATES_TIMEOUT),
        ),
    )


async def ensure_panel_message(bot: Bot, chat_id: int, ctrl: PanelControl) -> int:
    state: dict[str, int] = {}
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except (OSError, json.JSONDecodeError) as e:
            log.warning('state load failed: %s', e)
    key = str(chat_id)
    if key in state:
        return int(state[key])

    msg = await bot.send_message(
        chat_id=chat_id,
        text='<i>koori starting…</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=_keyboard(ctrl),
        disable_web_page_preview=True,
    )
    try:
        await bot.pin_chat_message(
            chat_id=chat_id, message_id=msg.message_id, disable_notification=True
        )
    except TelegramError as e:
        log.warning('pin failed (continuing anyway): %s', e)

    state[key] = msg.message_id
    STATE_FILE.write_text(json.dumps(state))
    return msg.message_id


async def wait_servers(sema: asyncio.Semaphore, iter: Iterable) -> None:
    for _ in iter:
        await sema.acquire()


async def render_loop(
    bot: Bot,
    chat_id: int,
    states: Sequence[ServerState],
    journal: deque[JournalEntry],
    ctrl: PanelControl,
    sema: asyncio.Semaphore,
) -> None:
    msg_id = await ensure_panel_message(bot, chat_id, ctrl)
    try:
        await asyncio.wait_for(wait_servers(sema, states), timeout=ctrl.interval)
    except TimeoutError:
        log.warning('timed out waiting for servers online')
    del sema

    while True:
        text = await render_panel(states, journal, ctrl)
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
            secs = wait.total_seconds() if isinstance(wait, timedelta) else float(wait)
            log.warning('rate-limited, sleeping %ss', secs)
            await asyncio.sleep(secs)
            continue
        except BadRequest as e:
            if 'not modified' not in str(e).lower():
                log.warning('edit failed: %s', e)
        except TelegramError as e:
            log.warning('edit failed: %s', e)

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(ctrl.refresh.wait(), timeout=ctrl.interval)
        ctrl.refresh.clear()


async def update_poller(bot: Bot, chat_id: int, ctrl: PanelControl) -> None:
    # Skip any pending updates from before this session.
    pending = await bot.get_updates(offset=-1, limit=1, timeout=0)
    offset = (pending[-1].update_id + 1) if pending else 0

    while True:
        try:
            updates = await bot.get_updates(
                offset=offset,
                timeout=LONG_POLL_TIMEOUT,
                allowed_updates=['callback_query'],
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
                case _:
                    pass
            log.debug('ctrl: interval=%g expanded=%s', ctrl.interval, ctrl.expanded)
            ctrl.refresh.set()
            await cb.answer()
