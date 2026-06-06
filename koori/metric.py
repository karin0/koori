import logging
import time

from typing import TYPE_CHECKING, NamedTuple

from prometheus_client.parser import text_string_to_metric_families

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .server import ServerState

log = logging.getLogger(__name__)

COLLECTORS = (
    'cpu',
    'meminfo',
    'netdev',
    'filesystem',
    'hwmon',
    'time',
    'loadavg',
    'conntrack',
    'vmstat',
    'netstat',
    'stat',
    'kernel_hung',
    'softnet',
)

ALERT_RULES = (
    ('node_vmstat_oom_kill', 'OOM'),
    ('node_kernel_hung_tasks_total', 'Hung'),
    ('node_netstat_TcpExt_ListenOverflows', 'ListenOvfl'),
    ('node_cpu_package_throttles_total', 'Throttle'),
    ('node_softnet_dropped_total', 'NetDrop'),
)

METRICS_KEYS = (
    'node_time_seconds',
    'node_scrape_collector_duration_seconds',
    'node_load1',
    'node_boot_time_seconds',
    'node_memory_MemTotal_bytes',
    'node_memory_MemAvailable_bytes',
    'node_memory_SwapTotal_bytes',
    'node_memory_SwapFree_bytes',
    'node_nf_conntrack_entries',
    'node_nf_conntrack_entries_limit',
    'node_memory_HardwareCorrupted_bytes',
    'node_procs_blocked',
    *tuple(k for k, _ in ALERT_RULES),
)

EXT_KEYS = (
    'node_cpu_seconds_total',
    'node_filesystem_size_bytes',
    'node_filesystem_avail_bytes',
    'node_network_receive_bytes_total',
    'node_network_transmit_bytes_total',
    'node_hwmon_temp_celsius',
    'node_scrape_collector_success',
)


class MetricsSnapshot(NamedTuple):
    timestamp: float
    scrape_time: float
    cpu_pct: float | None  # mean across cores
    cpu_max_pct: float | None  # busiest core
    cpu_count: int
    cpu_seconds: dict[str, tuple[float, float]]
    mem_used: int
    mem_total: int
    swap_used: int
    swap_total: int
    fs_used: int
    fs_total: int
    fs_mount: str | None
    rx: float
    tx: float
    rx_bps: float | None
    tx_bps: float | None
    temp: float | None
    temp_chip: str | None
    load1: float
    boot_time: float
    alerts: Sequence[str]
    cnt_alerts: Sequence[str]
    counters: dict[str, int]


def parse_metrics(state: ServerState, text: str) -> MetricsSnapshot:
    per_cpu_idle: dict[str, float] = {}
    per_cpu_total: dict[str, float] = {}
    rx_total = 0
    tx_total = 0
    fs_by_mount: dict[str, list[int]] = {}

    alerts: list[str] = []
    cnt_alerts: list[str] = []
    temp_max: float | None = None
    temp_chip: str | None = None

    # Pre-define the dict of concern to instantly filter out all other metrics in O(1)
    metrics: dict[str, float] = dict.fromkeys(METRICS_KEYS, 0)

    if log.isEnabledFor(logging.DEBUG):
        missing_keys = set(metrics)
        missing_keys.update(EXT_KEYS)
    else:
        missing_keys = None

    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if missing_keys:
                missing_keys.discard(s.name)
            match s.name:
                case 'node_cpu_seconds_total':
                    cpu_id = s.labels.get('cpu', '')
                    per_cpu_total[cpu_id] = per_cpu_total.get(cpu_id, 0) + s.value
                    if s.labels.get('mode') == 'idle':
                        per_cpu_idle[cpu_id] = per_cpu_idle.get(cpu_id, 0) + s.value
                case 'node_filesystem_size_bytes':
                    mp = s.labels.get('mountpoint', '')
                    fs_by_mount.setdefault(mp, [0, 0])[0] = int(s.value)
                case 'node_filesystem_avail_bytes':
                    mp = s.labels.get('mountpoint', '')
                    fs_by_mount.setdefault(mp, [0, 0])[1] = int(s.value)
                case 'node_network_receive_bytes_total':
                    if s.labels.get('device') != 'lo':
                        rx_total += s.value
                case 'node_network_transmit_bytes_total':
                    if s.labels.get('device') != 'lo':
                        tx_total += s.value
                case 'node_hwmon_temp_celsius':
                    if temp_max is None or s.value > temp_max:
                        temp_max = s.value
                        temp_chip = s.labels.get('chip')
                case 'node_scrape_collector_success':
                    if not s.value:
                        log.warning(
                            '%s: collector failed: %s', state.name, s.labels.get('collector')
                        )
                case _:
                    if (old := metrics.get(s.name)) is not None:
                        # Accumulate in case there are multiple samples (e.g. CPU package throttles)
                        metrics[s.name] = old + s.value

    if missing_keys:
        log.warning('%s: missing metrics: %s', state.name, missing_keys)

    mem_total = int(metrics['node_memory_MemTotal_bytes'])
    mem_available = int(metrics['node_memory_MemAvailable_bytes'])
    swap_total = int(metrics['node_memory_SwapTotal_bytes'])
    swap_free = int(metrics['node_memory_SwapFree_bytes'])

    if not (ts := metrics['node_time_seconds']):
        ts = time.time()
        log.warning('%s: missing node_time_seconds metric', state.name)

    if (blocked := int(metrics['node_procs_blocked'])) >= 10:
        alerts.append(f'Blocked: {blocked}')

    if (ct_limit := int(metrics['node_nf_conntrack_entries_limit'])) and (
        ct_entries := int(metrics['node_nf_conntrack_entries'])
    ) * 5 >= (ct_limit << 2):
        alerts.append(f'Conntrack: {ct_entries}/{ct_limit}')

    if corrupt_val := metrics['node_memory_HardwareCorrupted_bytes']:
        alerts.append(f'RAM Corrupted: {int(corrupt_val) >> 20} MiB')

    # Per-core rates; derive mean and max.
    cpu_pct: float | None = None
    cpu_max_pct: float | None = None

    # Network rates.
    rx_bps: float | None = None
    tx_bps: float | None = None

    if (prev := state.metrics) is None:
        counters = {name: int(metrics[name]) for name, _ in ALERT_RULES}
    else:
        counters = {}
        for name, label in ALERT_RULES:
            counters[name] = val = int(metrics[name])
            if val and (delta := val - prev.counters[name]):
                cnt_alerts.append(f'{label}: {val} ({delta:+})')

        prev_cpu = prev.cpu_seconds
        per_pcts: list[float] = []
        for cid, total in per_cpu_total.items():
            if (prev_info := prev_cpu.get(cid)) is not None:
                prev_idle, prev_total = prev_info
                if (td := total - prev_total) > 0:
                    per_pcts.append(max(0, 100 * (1 - (per_cpu_idle.get(cid, 0) - prev_idle) / td)))

        if per_pcts:
            cpu_pct = sum(per_pcts) / len(per_pcts)
            cpu_max_pct = max(per_pcts)

        if (dt := ts - prev.timestamp) > 0:
            rx_bps = max(0.0, (rx_total - prev.rx) / dt)
            tx_bps = max(0.0, (tx_total - prev.tx) / dt)

    cpu_seconds = {cid: (per_cpu_idle.get(cid, 0), t) for cid, t in per_cpu_total.items()}

    if (fs := fs_by_mount.get(fs_mount := '/')) is not None:
        fs_total, avail = fs
        fs_used = fs_total - avail
    else:
        fs_total = fs_used = 0
        fs_mount = None

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
            list(fs_by_mount),
            fs_used,
            fs_total,
        )

    return MetricsSnapshot(
        timestamp=ts,
        scrape_time=metrics['node_scrape_collector_duration_seconds'],
        cpu_pct=cpu_pct,
        cpu_max_pct=cpu_max_pct,
        cpu_count=len(per_cpu_total),
        cpu_seconds=cpu_seconds,
        mem_used=max(0, mem_total - mem_available),
        mem_total=mem_total,
        swap_used=max(0, swap_total - swap_free),
        swap_total=swap_total,
        fs_used=fs_used,
        fs_total=fs_total,
        fs_mount=fs_mount,
        rx=rx_total,
        tx=tx_total,
        rx_bps=rx_bps,
        tx_bps=tx_bps,
        load1=metrics['node_load1'],
        boot_time=metrics['node_boot_time_seconds'],
        temp=temp_max,
        temp_chip=temp_chip,
        alerts=alerts,
        cnt_alerts=cnt_alerts,
        counters=counters,
    )
