import asyncio
import logging
import time

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from prometheus_client.parser import text_string_to_metric_families

if TYPE_CHECKING:
    from collections import deque
    from collections.abc import Awaitable, Callable

    from .connecter import Connecter, Connection

log = logging.getLogger(__name__)

JOURNAL_BUFFER_SIZE = 30
RECONNECT_BACKOFF_MAX = 60.0
TEMP_ALERT_THRESHOLD = 80.0

type JournalEntry = str


@dataclass(slots=True, eq=False, match_args=False)
class MetricsSnapshot:
    timestamp: float
    cpu_pct: float | None  # mean across cores
    cpu_max_pct: float | None  # busiest core
    cpu_count: int
    mem_used: int
    mem_total: int
    swap_used: int
    swap_total: int
    fs_used: int
    fs_total: int
    fs_mount: str
    rx_bps: float | None
    tx_bps: float | None
    diagnostics: list[str] = field(default_factory=list)
    alerts: list[str] = field(default_factory=list)


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


def parse_metrics(text: str, state: ServerState, now: float) -> MetricsSnapshot:
    per_cpu_idle: dict[str, float] = {}
    per_cpu_total: dict[str, float] = {}
    mem_total = 0
    mem_available = 0
    swap_total = 0
    swap_free = 0
    rx_total = 0.0
    tx_total = 0.0
    fs_by_mount: dict[str, list[float]] = {}

    diagnostics: list[str] = []
    alerts: list[str] = []
    temp_max: float | None = None
    temp_chip: str | None = None
    boot_time: float | None = None
    throttles = 0
    conntrack_entries = 0
    conntrack_limit = 0
    softnet_drops = 0

    for family in text_string_to_metric_families(text):
        for s in family.samples:
            match s.name:
                case 'node_cpu_seconds_total':
                    cpu_id = s.labels.get('cpu', '')
                    per_cpu_total[cpu_id] = per_cpu_total.get(cpu_id, 0.0) + s.value
                    if s.labels.get('mode') == 'idle':
                        per_cpu_idle[cpu_id] = per_cpu_idle.get(cpu_id, 0.0) + s.value
                case 'node_memory_MemTotal_bytes':
                    mem_total = int(s.value)
                case 'node_memory_MemAvailable_bytes':
                    mem_available = int(s.value)
                case 'node_memory_SwapTotal_bytes':
                    swap_total = int(s.value)
                case 'node_memory_SwapFree_bytes':
                    swap_free = int(s.value)
                case 'node_filesystem_size_bytes':
                    mp = s.labels.get('mountpoint', '')
                    fs_by_mount.setdefault(mp, [0.0, 0.0])[0] = s.value
                case 'node_filesystem_avail_bytes':
                    mp = s.labels.get('mountpoint', '')
                    fs_by_mount.setdefault(mp, [0.0, 0.0])[1] = s.value
                case 'node_network_receive_bytes_total':
                    if s.labels.get('device') != 'lo':
                        rx_total += s.value
                case 'node_network_transmit_bytes_total':
                    if s.labels.get('device') != 'lo':
                        tx_total += s.value
                case 'node_load1':
                    diagnostics.append(f'Load: {s.value:.2f}')
                case 'node_boot_time_seconds':
                    boot_time = s.value
                case 'node_hwmon_temp_celsius':
                    if temp_max is None or s.value > temp_max:
                        temp_max = s.value
                        temp_chip = s.labels.get('chip')
                case 'node_nf_conntrack_entries':
                    conntrack_entries = int(s.value)
                case 'node_nf_conntrack_entries_limit':
                    conntrack_limit = int(s.value)
                case 'node_vmstat_oom_kill':
                    val = int(s.value)
                    if val > 0:
                        alerts.append(f'OOM: {val}')
                case 'node_kernel_hung_tasks_total':
                    val = int(s.value)
                    if val > 0:
                        alerts.append(f'Hung: {val}')
                case 'node_cpu_package_throttles_total':
                    throttles += int(s.value)
                case 'node_memory_HardwareCorrupted_bytes':
                    val = int(s.value)
                    if val > 0:
                        alerts.append(f'RAM Corrupted: {val >> 20} MiB')
                case 'node_procs_blocked':
                    val = int(s.value)
                    if val >= 10:
                        alerts.append(f'Blocked: {val}')
                case 'node_softnet_dropped_total':
                    softnet_drops += int(s.value)
                case 'node_netstat_TcpExt_ListenOverflows':
                    val = int(s.value)
                    if val > 0:
                        alerts.append(f'ListenOvfl: {val}')

    if boot_time is not None:
        up_secs = int(now - boot_time)
        days, rem = divmod(up_secs, 86400)
        hours = rem // 3600
        diagnostics.append(f'Up: {days}d {hours}h' if days else f'Up: {hours}h {rem % 3600 // 60}m')
    if temp_max is not None:
        if temp_max >= TEMP_ALERT_THRESHOLD:
            chip_suffix = f' ({temp_chip})' if temp_chip else ''
            diagnostics.append(f'Temp: {temp_max:.1f}°C{chip_suffix}')
            alerts.append(f'Temp High: {temp_max:.1f}°C')
        else:
            diagnostics.append(f'Temp: {temp_max:.1f}°C')
    if conntrack_limit > 0 and conntrack_entries / conntrack_limit >= 0.8:
        alerts.append(f'Conntrack: {conntrack_entries}/{conntrack_limit}')

    if throttles > 0:
        alerts.append(f'Throttle: {throttles}')
    if softnet_drops > 0:
        alerts.append(f'NetDrop: {softnet_drops}')

    # Per-core rates; derive mean and max.
    cpu_pct: float | None = None
    cpu_max_pct: float | None = None
    per_pcts: list[float] = []
    for cid, total in per_cpu_total.items():
        prev = state._prev_cpu.get(cid)
        if prev is None:
            continue
        prev_idle, prev_total = prev
        td = total - prev_total
        if td > 0:
            per_pcts.append(max(0.0, 100.0 * (1.0 - (per_cpu_idle.get(cid, 0.0) - prev_idle) / td)))
    if per_pcts:
        cpu_pct = sum(per_pcts) / len(per_pcts)
        cpu_max_pct = max(per_pcts)
    state._prev_cpu = {cid: (per_cpu_idle.get(cid, 0.0), t) for cid, t in per_cpu_total.items()}

    # Network rates.
    rx_bps: float | None = None
    tx_bps: float | None = None
    if state._prev_net is not None:
        prev_ts, prev_rx, prev_tx = state._prev_net
        dt = now - prev_ts
        if dt > 0:
            rx_bps = max(0.0, (rx_total - prev_rx) / dt)
            tx_bps = max(0.0, (tx_total - prev_tx) / dt)
    state._prev_net = (now, rx_total, tx_total)

    fs_total = 0
    fs_used = 0
    if '/' in fs_by_mount:
        size, avail = fs_by_mount['/']
        fs_total = int(size)
        fs_used = int(size - avail)

    if log.isEnabledFor(logging.DEBUG):
        log.debug(
            '%s: cpus=%d (avg=%s max=%s) mem=%d/%d swap=%d/%d fs_mounts=%s root_fs=%d/%d',
            state.name,
            len(per_cpu_total),
            f'{cpu_pct:.1f}' if cpu_pct is not None else 'NA',
            f'{cpu_max_pct:.1f}' if cpu_max_pct is not None else 'NA',
            mem_total - mem_available,
            mem_total,
            swap_total - swap_free,
            swap_total,
            list(fs_by_mount.keys()),
            fs_used,
            fs_total,
        )

    return MetricsSnapshot(
        timestamp=now,
        cpu_pct=cpu_pct,
        cpu_max_pct=cpu_max_pct,
        cpu_count=len(per_cpu_total),
        mem_used=max(0, mem_total - mem_available),
        mem_total=mem_total,
        swap_used=max(0, swap_total - swap_free),
        swap_total=swap_total,
        fs_used=fs_used,
        fs_total=fs_total,
        fs_mount='/',
        rx_bps=rx_bps,
        tx_bps=tx_bps,
        diagnostics=diagnostics,
        alerts=alerts,
    )


async def _journal_loop(
    conn: Connection,
    state: ServerState,
    journal: deque[JournalEntry],
) -> None:
    async for line in conn.journal_lines():
        if (line := line.strip()) and not any(kw in line for kw in state.exclude):
            journal.append(line)


async def run_server(
    server: ServerState, journal: deque[JournalEntry], sema: asyncio.Semaphore | None = None
) -> None:
    backoff = 1.0
    while True:
        try:
            async with server.connecter.open() as conn:

                async def _get_metrics() -> MetricsSnapshot:
                    text = await conn.fetch_metrics()
                    server.metrics = metrics = parse_metrics(text, server, time.time())
                    return metrics

                server.update = _get_metrics
                server.error = None
                backoff = 1.0
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
