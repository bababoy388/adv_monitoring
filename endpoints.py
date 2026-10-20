"""
Все HTTP и WebSocket endpoints.
"""

import asyncio
import csv
import io
import traceback
from pathlib import Path
from typing import List

import serial.tools.list_ports
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel

from config import BAUD_RATE, REFRESH_HZ
from state import multi_store
from parsers import parse_tx, parse_rx, reset_rx_state
from serial_reader import SerialReader
from data_logger import logger


router = APIRouter()

_readers: List[SerialReader] = []


def _stop_readers():
    global _readers
    for r in _readers:
        r.stop()
    for r in _readers:
        r.join(timeout=1.0)
    _readers = []
    # Сохраняем последние циклы, которые не успели закрыться новым ADV_START.
    with multi_store.lock:
        for port in multi_store.tx_order:
            store = multi_store.tx_stores.get(port)
            if not store:
                continue
            for cycle in store.cycles:
                if not getattr(cycle, "_csv_written", False):
                    if store.current is not cycle:
                        cycle.done = True
                    logger.write_cycle(store.label, store.port, store.ble_id, cycle)
                    cycle._csv_written = True
    logger.stop()


class StartReq(BaseModel):
    tx_ports: List[str]
    rx_port: str


@router.get("/")
async def index():
    path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(path.read_text(encoding="utf-8"))


@router.get("/api/ports")
async def ports():
    return [p.device for p in serial.tools.list_ports.comports()]


@router.post("/api/start")
async def start(req: StartReq):
    global _readers
    _stop_readers()
    multi_store.reset()
    reset_rx_state()

    if not req.tx_ports:
        return {"ok": False, "error": "Нужен хотя бы один TX порт"}

    if len(set(req.tx_ports)) != len(req.tx_ports):
        return {"ok": False, "error": "Дублирующиеся порты TX"}

    log_path = logger.start()

    for i, port in enumerate(req.tx_ports, 1):
        multi_store.add_tx(f"TX {i}", port)

    for i, port in enumerate(req.tx_ports, 1):
        def make_callbacks(p: str):
            def on_line(line: str):
                with multi_store.lock:
                    s = multi_store.tx_stores.get(p)
                    if s:
                        s.raw.append(line)
                parse_tx(line, p)

            def on_open():
                with multi_store.lock:
                    s = multi_store.tx_stores.get(p)
                    if s:
                        s.opened = True

            def on_error(msg: str):
                with multi_store.lock:
                    s = multi_store.tx_stores.get(p)
                    if s:
                        s.open_error = msg
                    multi_store.error = f"TX {p}: {msg}"
                    multi_store.running = False

            return on_line, on_open, on_error

        on_line, on_open, on_error = make_callbacks(port)
        r = SerialReader(port, BAUD_RATE, f"TX {i}", on_line,
                         on_open=on_open, on_error=on_error)
        _readers.append(r)
        r.start()

    def rx_on_line(line: str):
        with multi_store.lock:
            multi_store.raw_rx.append(line)
        parse_rx(line)

    def rx_on_open():
        with multi_store.lock:
            multi_store.rx_opened = True

    def rx_on_error(msg: str):
        with multi_store.lock:
            multi_store.rx_open_error = msg
            multi_store.error = f"RX {req.rx_port}: {msg}"
            multi_store.running = False

    multi_store.rx_port = req.rx_port
    r = SerialReader(req.rx_port, BAUD_RATE, "RX", rx_on_line,
                     on_open=rx_on_open, on_error=rx_on_error)
    _readers.append(r)
    r.start()

    with multi_store.lock:
        multi_store.running = True
    return {"ok": True, "csv_path": log_path}


@router.post("/api/stop")
async def stop():
    path = str(logger.path.resolve()) if logger.path else None
    _stop_readers()
    with multi_store.lock:
        multi_store.running = False
    return {"ok": True, "csv_path": path}


@router.get("/api/debug")
async def debug():
    return multi_store.debug_snapshot()


@router.get("/api/stats")
async def stats():
    with multi_store.lock:
        result = {"tx": []}
        for port in multi_store.tx_order:
            s = multi_store.tx_stores.get(port)
            if not s:
                continue
            total = s.sync_ok + s.fallback
            result["tx"].append({
                "label": s.label,
                "port": s.port,
                "ble_id": s.ble_id,
                "cycles": len(s.cycles),
                "sync_ok": s.sync_ok,
                "fallback": s.fallback,
                "success_pct": round(s.sync_ok / total * 100, 1) if total else 0.0,
                "rx_packets": s.rx_packets,
                "wakeups": s.wakeups,
                "drift_all": s.drift_stats(),
                "drift_recent_20": s.drift_stats_recent(20),
                "anomalies": s.anomalies(),
                "timing_stats": s.timing_stats(),
            })
        result["rx_timings"] = multi_store.rx_timing_store.stats()
        return result


@router.get("/api/export.csv")
async def export_csv():
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", lineterminator="\n")

    with multi_store.lock:
        # ─── Секция 1: циклы TX ───
        writer.writerow([
            "tx_label", "tx_port", "tx_ble_id",
            "num", "time_s", "slot", "adv_ms", "sync_ms",
            "fallback", "loss",
            "rx_t_in_cycle_ms", "rx_drift_ms", "rel_ms",
            "ema_ms", "ema_n", "ema_outliers", "comp_ms", "integral",
            "inside_slot", "skipped", "warmup_done", "reboot",
            "t_boot_ms", "timing_outliers",
            "status",
        ])

        for port in multi_store.tx_order:
            s = multi_store.tx_stores.get(port)
            if not s:
                continue
            for c in s.cycles:
                timing_outliers = sum(
                    1 for t in c.timings.values() if t.get("is_outlier")
                )
                writer.writerow([
                    s.label, s.port, s.ble_id or "",
                    c.num, f"{c.adv_start:.2f}",
                    c.rx_slot  if c.rx_slot  is not None else "",
                    c.adv_ms   if c.adv_ms   is not None else "",
                    c.sync_ms  if c.sync_ms  is not None else "",
                    1 if c.fallback else 0,
                    c.loss     if c.loss     is not None else "",
                    c.rx_t     if c.rx_t     is not None else "",
                    c.rx_drift if c.rx_drift is not None else "",
                    c.rel_ms   if c.rel_ms   is not None else "",
                    c.ema_ms   if c.ema_ms   is not None else "",
                    c.ema_n    if c.ema_n    is not None else "",
                    c.ema_outliers if c.ema_outliers is not None else "",
                    c.comp_ms  if c.comp_ms  is not None else "",
                    c.integral if c.integral is not None else "",
                    1 if c.inside_slot is True else (0 if c.inside_slot is False else ""),
                    1 if c.skipped_cycle else 0,
                    1 if c.warmup_done else 0,
                    1 if c.reboot else 0,
                    c.t_boot_ms if c.t_boot_ms is not None else "",
                    timing_outliers,
                    "done" if c.done else "running",
                ])

        # ─── Секция 2: RX timings ───
        rx_stats = multi_store.rx_timing_store.stats()

        writer.writerow([])
        writer.writerow(["=== RX TIMINGS ==="])
        writer.writerow([
            "name", "n", "last_us", "min_us",
            "avg_us", "max_us", "outliers", "last_ts_s",
        ])

        if rx_stats:
            for name in sorted(rx_stats.keys()):
                s = rx_stats[name]
                writer.writerow([
                    name,
                    s["n"],
                    s["last"],
                    s["min"],
                    s["avg"],
                    s["max"],
                    s["outliers"],
                    s["last_ts"],
                ])
        else:
            writer.writerow(["(нет данных — RX не выдаёт [TIME] метки)"])

    csv_data = "\ufeff" + buf.getvalue()
    return Response(
        content=csv_data.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="ble_cycles.csv"'},
    )


@router.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket.accept()
    print("[WS] client connected")
    try:
        while True:
            snap = multi_store.snapshot()
            await websocket.send_json(snap)
            await asyncio.sleep(1.0 / REFRESH_HZ)
    except WebSocketDisconnect:
        print("[WS] client disconnected")
    except Exception as e:
        print(f"[WS] ERROR: {type(e).__name__}: {e}")
        traceback.print_exc()