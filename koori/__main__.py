import argparse
import asyncio
import logging
import os
import tomllib

from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .bot import PanelControl, build_bot, render_loop, update_poller
from .connecter import LocalConnecter, SSHConnecter
from .panel import render_panel
from .server import JOURNAL_BUFFER_SIZE, JournalEntry, ServerState, run_server

if TYPE_CHECKING:
    from collections.abc import Coroutine, Sequence

    from telegram import Bot

log = logging.getLogger(__name__)


def _build_server(spec: dict[str, Any]) -> ServerState:
    name = spec['name']

    match spec.get('kind'):
        case 'local':
            connector = LocalConnecter()
        case 'ssh' | None:
            connector = SSHConnecter(spec.get('host', name))
        case other:
            raise ValueError(f'unknown connecter kind: {other!r}')

    return ServerState(name, connector, exclude=list(spec.get('exclude') or []))


async def _dry_run(
    servers: Sequence[ServerState], journal: deque[JournalEntry], ctrl: PanelControl
) -> None:
    # ruff: noqa: T201
    tasks = tuple(asyncio.create_task(run_server(s, journal)) for s in servers)
    try:
        while True:
            print('\x1b[2J\x1b[H', end='')
            print(await render_panel(servers, journal, ctrl), flush=True)
            await asyncio.sleep(2)
    finally:
        for t in tasks:
            t.cancel()


async def wait_tasks(bot: Bot, tasks: Sequence[asyncio.Task]) -> None:
    async with bot:
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()


def run(config_path: Path, *, dry_run: bool) -> Coroutine[Any, Any, None]:
    config = tomllib.loads(config_path.read_text())

    if not (specs := config.get('servers')):
        raise RuntimeError('no servers configured')

    servers = tuple(_build_server(spec) for spec in specs)
    journal: deque[JournalEntry] = deque(maxlen=JOURNAL_BUFFER_SIZE)
    ctrl = PanelControl()

    if dry_run:
        return _dry_run(servers, journal, ctrl)

    chat_id = int(config['chat_id'])
    log.info('panel in chat=%s', chat_id)

    bot = build_bot()
    sema = asyncio.Semaphore(0)
    tasks = (
        *(
            asyncio.create_task(run_server(s, journal, sema), name=f'server:{s.name}')
            for s in servers
        ),
        asyncio.create_task(render_loop(bot, chat_id, servers, journal, ctrl, sema), name='render'),
        asyncio.create_task(update_poller(bot, chat_id, ctrl), name='poller'),
    )
    return wait_tasks(bot, tasks)


def entry() -> Coroutine[Any, Any, None]:
    parser = argparse.ArgumentParser()
    parser.add_argument('config', type=Path, default=Path('config.toml'), nargs='?')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    if 'JOURNAL_STREAM' in os.environ:
        fmt = '[%(levelname)s] %(message)s'
    else:
        fmt = '%(asctime)s %(levelname)-5s %(name)s: %(message)s'

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format=fmt)
    logging.getLogger('asyncssh').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)

    return run(args.config, dry_run=args.dry_run)


async def main() -> None:
    await entry()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info('Interrupted')
