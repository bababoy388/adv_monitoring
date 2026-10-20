"""
Парсеры логов ESP-IDF. Каждый TX имеет свой порт.
RX-пакеты раздаются по BLE ID соответствующему TX.
"""

import re

from state import multi_store, Cycle
from config import TX_SLOT_WIDTH_MS, SYSTEM_CYCLE_MS, SYNC_TARGET_DRIFT_MS
from data_logger import logger


# ─── Regex: TX ───
RE_ADV_START      = re.compile(r'Starting advertising for 1 instances')
RE_ADV_STOP       = re.compile(r'Stopping advertising for instance 1')
RE_ADV_TERMINATED = re.compile(r'Advertising terminated, status 60')
RE_SYNC_FULL      = re.compile(r'Received sleep duration:\s*(\d+)')
RE_SLEEP_DURATION = re.compile(r'sleep_duration:\s*(\d+)\s*ms')
RE_WAKEUP         = re.compile(r'rst:0x5 \(SLEEP_WAKEUP\)')
RE_TX_ID          = re.compile(r'Transmitter ID:\s*([0-9a-fA-F]+)')

# ─── Regex: TX timing ───
RE_TIME_MARK = re.compile(
    r'\[TIME\]\s+(\S+?):\s*(\d+)\s*us(?:\s*\((\d+)\s*ms\))?'
)
RE_T_BOOT = re.compile(r'T_boot:\s*(\d+)\s*ms')

# ─── Regex: RX (базовая) ───
RE_RX_PACKET = re.compile(
    r'RX:\s*id=0x([0-9a-fA-F]+)\s+idx=(\d+)\s+t_in_cycle=(\d+)\s+sync=(\d+)'
)

# ─── Regex: RX (расширенная, с EMA) ───
RE_SYNC_DETAIL = re.compile(
    r'SYNC \[ID:0x([0-9a-fA-F]+)\]:\s*'
    r'Now:(\d+)\s*ms,\s*'
    r'SlotOffset:(\d+),\s*'
    r'Rel:(-?\d+),\s*'
    r'EMA:([-\d.]+)\s*\(n=(\d+),\s*out=(\d+)\),\s*'
    r'I:([-\d.]+),\s*'
    r'Comp:(-?\d+),\s*'
    r'PAYLOAD:(\d+)\s*ms'
)

# ─── Regex: RX (legacy) ───
RE_SYNC_DETAIL_LEGACY = re.compile(
    r'SYNC \[ID:0x([0-9a-fA-F]+)\]:\s*'
    r'Now:(\d+)\s*ms,\s*'
    r'SlotOffset:(\d+),\s*'
    r'PAYLOAD:(\d+)\s*ms'
)

# ─── Regex: аномалии ───
RE_OUTLIER = re.compile(
    r'Drift outlier on slot (\d+):\s*'
    r'sample=(-?\d+),\s*ema=([-\d.]+),\s*delta=([\d.]+)'
    r'(?:,\s*total outliers=(\d+))?'
)
RE_SKIPPED_CYCLE = re.compile(r'Target wake time passed! Skipped to next cycle')
RE_OUTSIDE_SLOT  = re.compile(r'Packet OUTSIDE SLOT! \(rel_time:\s*(-?\d+)\s*ms')
RE_INSIDE_SLOT   = re.compile(r'Packet inside SLOT range \(rel_time:\s*(-?\d+)\s*ms')
RE_WARMUP_START  = re.compile(r'Drift warmup:\s*slot (\d+)\s*—\s*(\d+)/(\d+)')
RE_WARMUP_DONE   = re.compile(r'Drift warmup complete for slot (\d+)')
RE_EMA_RESET     = re.compile(r'EMA reset on slot (\d+) after (\d+) outliers')
RE_LOSS          = re.compile(r'ID:\s*0x([0-9a-fA-F]+),\s*LOSS:\s*(\d+)')
RE_REBOOT        = re.compile(r'Tx (\d+) reboot detected, total reboots:\s*(\d+)')


# Буфер [TIME] меток, ожидающих привязки к циклу.
# Метки от RX приходят до строки "RX: id=...", поэтому копим их
# и привязываем к текущему циклу, когда узнаём id.
_pending_rx_timings: list = []
_PENDING_RX_MAX = 50


def reset_rx_state():
    """Сброс состояния парсера RX — вызывать при /api/start."""
    _pending_rx_timings.clear()


def _find_tx(tx_id: str):
    for s in multi_store.tx_stores.values():
        if s.ble_id == tx_id:
            return s
    if len(multi_store.tx_stores) == 1:
        return next(iter(multi_store.tx_stores.values()))
    return None


def _current_cycle(s):
    if s.current:
        return s.current
    if s.cycles:
        return s.cycles[-1]
    return None


def parse_tx(line: str, port: str) -> None:
    """Обработка строки от конкретного TX."""
    with multi_store.lock:
        s = multi_store.tx_stores.get(port)
        if s is None:
            return

        m = RE_TX_ID.search(line)
        if m and s.ble_id is None:
            s.ble_id = m.group(1).lower()
            print(f"[{s.label}] detected BLE ID: 0x{s.ble_id}")

        if RE_WAKEUP.search(line):
            s.wakeups += 1
            s.parsed_lines += 1
            return

        # ── [TIME] метки ──
        m = RE_TIME_MARK.search(line)
        if m:
            name = m.group(1)
            us = int(m.group(2))
            ms = int(m.group(3)) if m.group(3) else None
            evt = s.register_timing(name, us, ms, multi_store.now())
            payload = {"us": us, "ms": ms, "is_outlier": evt.is_outlier}
            if s.current:
                s.current.timings[name] = payload
            else:
                s.pending_timings[name] = payload
            s.parsed_lines += 1
            return

        # ── T_boot ──
        m = RE_T_BOOT.search(line)
        if m:
            if s.current:
                s.current.t_boot_ms = int(m.group(1))
            s.parsed_lines += 1
            return

        if RE_ADV_START.search(line):
            previous = s.current or (s.cycles[-1] if s.cycles else None)
            if previous and not previous.done:
                previous.done = True
            if previous and not getattr(previous, "_csv_written", False):
                logger.write_cycle(s.label, s.port, s.ble_id, previous)
                previous._csv_written = True
            s.cycles_total += 1
            c = Cycle(num=s.cycles_total, adv_start=multi_store.now())
            if s.pending_timings:
                c.timings.update(s.pending_timings)
                s.pending_timings = {}
            s.cycles.append(c)
            s.current = c
            s.parsed_lines += 1
            return

        if RE_ADV_TERMINATED.search(line) or RE_ADV_STOP.search(line):
            if s.current and s.current.adv_stop is None:
                s.current.adv_stop = multi_store.now()
                s.current.adv_ms = int((s.current.adv_stop - s.current.adv_start) * 1000)
            s.parsed_lines += 1
            return

        m = RE_SYNC_FULL.search(line)
        if m and s.current:
            s.current.sync_ms = int(m.group(1))
            s.parsed_lines += 1
            return

        m = RE_SLEEP_DURATION.search(line)
        if m and s.current:
            dur = int(m.group(1))
            if dur == 0:
                s.current.fallback = True
                s.fallback += 1
            else:
                s.current.sync_ms = dur
                s.sync_ok += 1
            s.current.done = True
            s.current = None
            s.parsed_lines += 1


def parse_rx(line: str) -> None:
    """Обработка строки от RX. Роутим по BLE ID."""

    # ─── 0. [TIME] метки от RX — регистрируем в глобальном store ───
    m = RE_TIME_MARK.search(line)
    if m:
        name = m.group(1)
        us = int(m.group(2))
        ms = int(m.group(3)) if m.group(3) else None
        with multi_store.lock:
            multi_store.rx_parsed_lines += 1
            multi_store.rx_timing_store.register(name, us, ms, multi_store.now())

            # Только per-packet метки привязываем к текущему циклу TX.
            # Boot-фазы и scan-control остаются только в глобальной таблице.
            if name.startswith("rx_proc/") or name.startswith("finalize/"):
                if len(_pending_rx_timings) < _PENDING_RX_MAX:
                    _pending_rx_timings.append({"name": name, "us": us, "ms": ms})
        return

    # ─── 1. Расширенная SYNC-строка ───
    m = RE_SYNC_DETAIL.search(line)
    if m:
        tx_id = m.group(1).lower()
        with multi_store.lock:
            multi_store.last_rx_id = tx_id
            multi_store.rx_parsed_lines += 1
            s = _find_tx(tx_id)
            if s:
                c = _current_cycle(s)
                if c:
                    c.rel_ms       = int(m.group(4))
                    c.ema_ms       = float(m.group(5))
                    c.ema_n        = int(m.group(6))
                    c.ema_outliers = int(m.group(7))
                    c.integral     = float(m.group(8))
                    c.comp_ms      = int(m.group(9))
                    if c.sync_ms is None:
                        c.sync_ms = int(m.group(10))
        return

    # ─── 2. Legacy SYNC-строка ───
    m = RE_SYNC_DETAIL_LEGACY.search(line)
    if m:
        tx_id = m.group(1).lower()
        with multi_store.lock:
            multi_store.last_rx_id = tx_id
            multi_store.rx_parsed_lines += 1
            s = _find_tx(tx_id)
            if s:
                c = _current_cycle(s)
                if c:
                    c.rel_ms = int(m.group(4))
                    if c.sync_ms is None:
                        c.sync_ms = int(m.group(5))
        return

    # ─── 3. Простой RX-пакет ───
    m = RE_RX_PACKET.search(line)
    if m:
        tx_ble_id = m.group(1).lower()
        idx  = int(m.group(2))
        t    = int(m.group(3))
        sync = int(m.group(4))

        drift = t - idx * TX_SLOT_WIDTH_MS
        if drift > SYSTEM_CYCLE_MS // 2:
            drift -= SYSTEM_CYCLE_MS
        if drift < -(SYSTEM_CYCLE_MS // 2):
            drift += SYSTEM_CYCLE_MS
        drift -= SYNC_TARGET_DRIFT_MS

        with multi_store.lock:
            multi_store.last_rx_id = tx_ble_id
            multi_store.rx_packets_total += 1
            multi_store.rx_last_packet_ts = multi_store.now()
            multi_store.rx_parsed_lines += 1
            s = _find_tx(tx_ble_id)
            if s:
                s.rx_packets += 1
                s.last_rx_packet_ts = multi_store.now()
                c = _current_cycle(s)
                if c:
                    c.rx_slot  = idx
                    c.rx_t     = t
                    c.rx_drift = drift
                    if c.sync_ms is None:
                        c.sync_ms = sync

                    # Привязываем накопленные per-packet [TIME] метки от RX
                    for tm in _pending_rx_timings:
                        c.timings[tm["name"]] = {
                            "us": tm["us"],
                            "ms": tm["ms"],
                            "is_outlier": False,
                        }
        _pending_rx_timings.clear()
        return

    # ─── 4. Аномалии ───
    with multi_store.lock:
        last_id = multi_store.last_rx_id

        if RE_OUTLIER.search(line):
            s = _find_tx(last_id)
            if s:
                s.outliers += 1
                s.parsed_lines += 1
            multi_store.rx_parsed_lines += 1
            return

        if RE_SKIPPED_CYCLE.search(line):
            s = _find_tx(last_id)
            if s:
                s.skipped_cycles += 1
                c = _current_cycle(s)
                if c:
                    c.skipped_cycle = True
                s.parsed_lines += 1
            multi_store.rx_parsed_lines += 1
            return

        if RE_OUTSIDE_SLOT.search(line):
            s = _find_tx(last_id)
            if s:
                s.outside_slot += 1
                c = _current_cycle(s)
                if c:
                    c.inside_slot = False
                s.parsed_lines += 1
            multi_store.rx_parsed_lines += 1
            return

        if RE_INSIDE_SLOT.search(line):
            s = _find_tx(last_id)
            if s:
                c = _current_cycle(s)
                if c:
                    c.inside_slot = True
                s.parsed_lines += 1
            multi_store.rx_parsed_lines += 1
            return

        if RE_WARMUP_DONE.search(line):
            s = _find_tx(last_id)
            if s:
                c = _current_cycle(s)
                if c:
                    c.warmup_done = True
                s.parsed_lines += 1
            multi_store.rx_parsed_lines += 1
            return

        if RE_EMA_RESET.search(line):
            s = _find_tx(last_id)
            if s:
                s.ema_resets += 1
                s.parsed_lines += 1
            multi_store.rx_parsed_lines += 1
            return

        m = RE_LOSS.search(line)
        if m:
            tx_id = m.group(1).lower()
            s = _find_tx(tx_id)
            if s:
                c = _current_cycle(s)
                if c:
                    c.loss = int(m.group(2))
                s.parsed_lines += 1
            multi_store.rx_parsed_lines += 1
            return

        if RE_REBOOT.search(line):
            s = _find_tx(last_id)
            if s:
                s.reboots += 1
                c = _current_cycle(s)
                if c:
                    c.reboot = True
                s.parsed_lines += 1
            multi_store.rx_parsed_lines += 1
            return