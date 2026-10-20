"""
Глобальное состояние: модели и мульти-TX хранилище.
"""

import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict

from config import MAX_ROWS, RAW_LINES


@dataclass
class TimingEvent:
    name: str
    us: int
    ts: float
    ms: Optional[int] = None
    is_outlier: bool = False


@dataclass
class Cycle:
    num: int
    adv_start: float
    adv_stop: Optional[float] = None
    adv_ms: Optional[int] = None
    sync_ms: Optional[int] = None
    fallback: bool = False
    done: bool = False

    rx_slot: Optional[int] = None
    rx_t: Optional[int] = None
    rx_drift: Optional[int] = None

    rel_ms: Optional[int] = None
    ema_ms: Optional[float] = None
    ema_n: Optional[int] = None
    ema_outliers: Optional[int] = None
    comp_ms: Optional[int] = None
    integral: Optional[float] = None

    inside_slot: Optional[bool] = None
    skipped_cycle: bool = False
    warmup_done: bool = False
    loss: Optional[int] = None
    reboot: bool = False

    timings: Dict[str, dict] = field(default_factory=dict)
    t_boot_ms: Optional[int] = None


class RxTimingStore:
    """
    Глобальное хранилище временных меток от приёмника.
    Метки не привязаны к конкретному TX — это внутренние фазы обработки RX.
    """

    HISTORY_SIZE = 200
    OUTLIER_SIGMA = 3.0
    OUTLIER_MIN_STD_US = 100

    def __init__(self):
        self.history: Dict[str, deque] = {}
        self.outlier_count: Dict[str, int] = {}
        self.total_count: Dict[str, int] = {}
        self.last_ts: Dict[str, float] = {}
        self.outlier_total = 0

    def register(self, name: str, us: int,
                 ms: Optional[int], ts: float) -> bool:
        h = self.history.setdefault(name, deque(maxlen=self.HISTORY_SIZE))
        self.total_count[name] = self.total_count.get(name, 0) + 1

        is_outlier = False
        if len(h) >= 5:
            vals = list(h)
            mean = sum(vals) / len(vals)
            var = sum((x - mean) ** 2 for x in vals) / len(vals)
            std = var ** 0.5
            if std > self.OUTLIER_MIN_STD_US and us > mean + self.OUTLIER_SIGMA * std:
                is_outlier = True

        h.append(us)
        if is_outlier:
            self.outlier_count[name] = self.outlier_count.get(name, 0) + 1
            self.outlier_total += 1
        self.last_ts[name] = ts
        return is_outlier

    def stats(self) -> Dict[str, dict]:
        out = {}
        for name, h in self.history.items():
            if not h:
                continue
            vals = list(h)
            out[name] = {
                "n": self.total_count.get(name, len(vals)),
                "recent_n": len(vals),
                "last": vals[-1],
                "min": min(vals),
                "max": max(vals),
                "avg": round(sum(vals) / len(vals)),
                "outliers": self.outlier_count.get(name, 0),
                "last_ts": round(self.last_ts.get(name, 0), 2),
            }
        return out

    def reset(self):
        self.history.clear()
        self.outlier_count.clear()
        self.total_count.clear()
        self.last_ts.clear()
        self.outlier_total = 0


class TxStore:
    """Состояние одного передатчика."""

    TIMING_HISTORY_SIZE = 30
    OUTLIER_SIGMA = 3.0
    OUTLIER_MIN_STD_US = 100
    NON_CYCLIC_PREFIXES = ("ble_init/", "start_scan/", "adv_init/")

    def __init__(self, label: str, port: str):
        self.label = label
        self.port = port
        self.ble_id: Optional[str] = None
        self.cycles = deque(maxlen=MAX_ROWS)
        self.cycles_total = 0
        self.current: Optional[Cycle] = None
        self.sync_ok = 0
        self.fallback = 0
        self.wakeups = 0
        self.rx_packets = 0
        self.last_rx_packet_ts: Optional[float] = None
        self.raw = deque(maxlen=RAW_LINES)

        self.skipped_cycles = 0
        self.outside_slot = 0
        self.outliers = 0
        self.ema_resets = 0
        self.reboots = 0

        self.timing_history: Dict[str, deque] = {}
        self.timing_outlier_count = 0
        self.pending_timings: Dict[str, dict] = {}

        self.bytes_in = 0
        self.lines_in = 0
        self.parsed_lines = 0
        self.opened = False
        self.open_error: Optional[str] = None

    def register_timing(self, name: str, us: int,
                        ms: Optional[int], ts: float) -> TimingEvent:
        hist = self.timing_history.setdefault(
            name, deque(maxlen=self.TIMING_HISTORY_SIZE)
        )

        is_non_cyclic = name.startswith(self.NON_CYCLIC_PREFIXES)

        is_outlier = False
        if not is_non_cyclic and len(hist) >= 5:
            mean = sum(hist) / len(hist)
            var = sum((x - mean) ** 2 for x in hist) / len(hist)
            std = var ** 0.5
            if std > self.OUTLIER_MIN_STD_US and us > mean + self.OUTLIER_SIGMA * std:
                is_outlier = True

        hist.append(us)
        if is_outlier:
            self.timing_outlier_count += 1
        return TimingEvent(name=name, us=us, ms=ms, ts=ts, is_outlier=is_outlier)

    def timing_stats(self) -> Dict[str, dict]:
        out = {}
        for name, hist in self.timing_history.items():
            if not hist:
                continue
            vals = list(hist)
            out[name] = {
                "n": len(vals),
                "avg": round(sum(vals) / len(vals)),
                "min": min(vals),
                "max": max(vals),
                "last": vals[-1],
            }
        return out

    def drift_stats(self) -> Dict:
        vals = [c.rx_drift for c in self.cycles if c.rx_drift is not None]
        if not vals:
            return {"n": 0, "avg": None, "std": None, "min": None, "max": None}
        return {
            "n": len(vals),
            "avg": round(sum(vals) / len(vals), 1),
            "std": round(statistics.pstdev(vals), 1) if len(vals) > 1 else 0.0,
            "min": min(vals),
            "max": max(vals),
        }

    def drift_stats_recent(self, n: int = 20) -> Dict:
        vals = [c.rx_drift for c in self.cycles if c.rx_drift is not None][-n:]
        if not vals:
            return {"n": 0, "avg": None, "std": None, "min": None, "max": None}
        return {
            "n": len(vals),
            "avg": round(sum(vals) / len(vals), 1),
            "std": round(statistics.pstdev(vals), 1) if len(vals) > 1 else 0.0,
            "min": min(vals),
            "max": max(vals),
        }

    def anomalies(self) -> Dict:
        return {
            "skipped_cycles": self.skipped_cycles,
            "outside_slot": self.outside_slot,
            "outliers": self.outliers,
            "ema_resets": self.ema_resets,
            "reboots": self.reboots,
            "timing_outliers": self.timing_outlier_count,
        }


class MultiStore:
    """Общее хранилище: много TX + один RX."""

    def __init__(self):
        self.lock = threading.Lock()
        self.t0 = time.monotonic()
        self.tx_stores: Dict[str, TxStore] = {}
        self.tx_order: List[str] = []
        self.raw_rx = deque(maxlen=RAW_LINES)
        self.rx_packets_total = 0
        self.rx_last_line: Optional[str] = None
        self.rx_last_line_ts: Optional[float] = None
        self.rx_last_packet_ts: Optional[float] = None
        self.running = False
        self.error: Optional[str] = None
        self.last_rx_id: Optional[str] = None

        self.rx_timing_store = RxTimingStore()

        self.rx_bytes_in = 0
        self.rx_lines_in = 0
        self.rx_parsed_lines = 0
        self.rx_opened = False
        self.rx_open_error: Optional[str] = None
        self.rx_port = ""

    def now(self):
        return time.monotonic() - self.t0

    def reset(self):
        with self.lock:
            self.t0 = time.monotonic()
            self.tx_stores = {}
            self.tx_order = []
            self.raw_rx = deque(maxlen=RAW_LINES)
            self.rx_packets_total = 0
            self.rx_last_line = None
            self.rx_last_line_ts = None
            self.rx_last_packet_ts = None
            self.running = False
            self.error = None
            self.last_rx_id = None
            self.rx_timing_store.reset()
            self.rx_bytes_in = 0
            self.rx_lines_in = 0
            self.rx_parsed_lines = 0
            self.rx_opened = False
            self.rx_open_error = None
            self.rx_port = ""

    def add_tx(self, label: str, port: str):
        with self.lock:
            if port not in self.tx_stores:
                self.tx_stores[port] = TxStore(label, port)
                self.tx_order.append(port)

    def snapshot(self):
        with self.lock:
            tx_list = []
            for port in self.tx_order:
                s = self.tx_stores.get(port)
                if s is None:
                    continue
                total = s.sync_ok + s.fallback
                ratio = (s.sync_ok / total * 100) if total > 0 else 0.0
                tx_list.append({
                    "label": s.label,
                    "port": s.port,
                    "ble_id": s.ble_id,
                    "stats": {
                        "sync_ok": s.sync_ok,
                        "fallback": s.fallback,
                        "success": round(ratio, 1),
                        "rx_packets": s.rx_packets,
                        "last_rx_packet_ts": getattr(s, "last_rx_packet_ts", None),
                        "wakeups": s.wakeups,
                        "cycles": s.cycles_total,
                    },
                    "drift": s.drift_stats(),
                    "drift_recent": s.drift_stats_recent(20),
                    "anomalies": s.anomalies(),
                    "timing_stats": s.timing_stats(),
                    "cycles": [asdict(c) for c in list(s.cycles)[-MAX_ROWS:]],
                    "raw": list(s.raw)[-RAW_LINES:],
                })

            return {
                "running": self.running,
                "error": self.error,
                "rx_packets_total": self.rx_packets_total,
                "rx_status": {
                    "port": self.rx_port,
                    "opened": self.rx_opened,
                    "open_error": self.rx_open_error,
                    "bytes_in": self.rx_bytes_in,
                    "lines_in": self.rx_lines_in,
                    "parsed_lines": self.rx_parsed_lines,
                    "last_line": self.rx_last_line,
                    "last_line_ts": self.rx_last_line_ts,
                    "last_packet_ts": self.rx_last_packet_ts,
                    "now": self.now(),
                },
                "tx": tx_list,
                "raw_rx": list(self.raw_rx)[-RAW_LINES:],
                "rx_timings": self.rx_timing_store.stats(),
            }

    def debug_snapshot(self):
        with self.lock:
            tx_debug = []
            for port in self.tx_order:
                s = self.tx_stores.get(port)
                if s is None:
                    continue
                tx_debug.append({
                    "label": s.label,
                    "port": s.port,
                    "ble_id": s.ble_id,
                    "opened": s.opened,
                    "open_error": s.open_error,
                    "bytes_in": s.bytes_in,
                    "lines_in": s.lines_in,
                    "parsed_lines": s.parsed_lines,
                    "cycles": s.cycles_total,
                    "last_raw": list(s.raw)[-15:],
                })

            return {
                "running": self.running,
                "error": self.error,
                "rx": {
                    "port": self.rx_port,
                    "opened": self.rx_opened,
                    "open_error": self.rx_open_error,
                    "bytes_in": self.rx_bytes_in,
                    "lines_in": self.rx_lines_in,
                    "parsed_lines": self.rx_parsed_lines,
                    "packets": self.rx_packets_total,
                    "last_line": self.rx_last_line,
                    "last_line_ts": self.rx_last_line_ts,
                    "last_packet_ts": self.rx_last_packet_ts,
                    "last_raw": list(self.raw_rx)[-15:],
                    "timings_count": len(self.rx_timing_store.history),
                },
                "tx": tx_debug,
            }


multi_store = MultiStore()