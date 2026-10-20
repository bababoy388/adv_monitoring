"""Потоковая запись структурированных циклов TX в CSV."""
import csv
import threading
from datetime import datetime
from pathlib import Path

CYCLE_HEADER = [
    "tx_label", "tx_port", "tx_ble_id", "num", "time_s", "slot", "adv_ms", "sync_ms",
    "fallback", "loss", "rx_t_in_cycle_ms", "rx_drift_ms", "rel_ms", "ema_ms", "ema_n",
    "ema_outliers", "comp_ms", "integral", "inside_slot", "skipped", "warmup_done",
    "reboot", "t_boot_ms", "timing_outliers", "status",
]


def cycle_row(label, port, ble_id, c):
    timing_outliers = sum(1 for t in c.timings.values() if t.get("is_outlier"))
    return [
        label, port, ble_id or "", c.num, f"{c.adv_start:.2f}",
        c.rx_slot if c.rx_slot is not None else "",
        c.adv_ms if c.adv_ms is not None else "",
        c.sync_ms if c.sync_ms is not None else "",
        1 if c.fallback else 0,
        c.loss if c.loss is not None else "",
        c.rx_t if c.rx_t is not None else "",
        c.rx_drift if c.rx_drift is not None else "",
        c.rel_ms if c.rel_ms is not None else "",
        c.ema_ms if c.ema_ms is not None else "",
        c.ema_n if c.ema_n is not None else "",
        c.ema_outliers if c.ema_outliers is not None else "",
        c.comp_ms if c.comp_ms is not None else "",
        c.integral if c.integral is not None else "",
        1 if c.inside_slot is True else (0 if c.inside_slot is False else ""),
        1 if c.skipped_cycle else 0,
        1 if c.warmup_done else 0,
        1 if c.reboot else 0,
        c.t_boot_ms if c.t_boot_ms is not None else "",
        timing_outliers,
        "done" if c.done else "running",
    ]


class DataLogger:
    def __init__(self):
        self._lock = threading.Lock()
        self._file = None
        self._writer = None
        self.session_dir = None
        self.path = None

    def start(self):
        with self._lock:
            self._close_unlocked()
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            self.session_dir = Path('data') / f'session_{stamp}'
            self.session_dir.mkdir(parents=True, exist_ok=False)
            self.path = self.session_dir / 'ble_cycles.csv'
            self._file = self.path.open('w', newline='', encoding='utf-8-sig', buffering=1)
            self._writer = csv.writer(self._file, delimiter=';', lineterminator='\n')
            self._writer.writerow(CYCLE_HEADER)
            self._file.flush()
            return str(self.path.resolve())

    def write_cycle(self, label, port, ble_id, cycle):
        with self._lock:
            if not self._writer or not self._file:
                return
            self._writer.writerow(cycle_row(label, port, ble_id, cycle))
            self._file.flush()

    def stop(self):
        with self._lock:
            self._close_unlocked()

    def _close_unlocked(self):
        if self._file:
            try:
                self._file.flush()
                self._file.close()
            finally:
                self._file = None
                self._writer = None


logger = DataLogger()
