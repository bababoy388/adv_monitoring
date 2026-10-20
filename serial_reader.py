"""
Поток-читатель COM-порта.
"""

import threading
from typing import Callable, Optional

import serial

from state import multi_store


class SerialReader(threading.Thread):
    def __init__(self, port: str, baud: int, name: str,
                 on_line: Callable[[str], None],
                 on_open: Optional[Callable[[], None]] = None,
                 on_error: Optional[Callable[[str], None]] = None):
        super().__init__(daemon=True)
        self.port = port
        self.baud = baud
        self.name = name
        self.on_line = on_line
        self.on_open = on_open
        self.on_error = on_error
        self.stop_event = threading.Event()
        self.ser: Optional[serial.Serial] = None

    def run(self):
        try:
            self.ser = serial.Serial(self.port, self.baud, timeout=0.1)
            print(f"[{self.name}] opened {self.port} @ {self.baud}")
            if self.on_open:
                self.on_open()
        except Exception as e:
            print(f"[{self.name}] open failed: {e}")
            if self.on_error:
                self.on_error(str(e))
            return

        buffer = b""
        while not self.stop_event.is_set():
            try:
                data = self.ser.read(self.ser.in_waiting or 1)
            except serial.SerialException as e:
                if self.on_error:
                    self.on_error(str(e))
                break
            if not data:
                continue

            # диагностика: байты
            with multi_store.lock:
                if self.name == "RX":
                    multi_store.rx_bytes_in += len(data)
                else:
                    for s in multi_store.tx_stores.values():
                        if s.port == self.port:
                            s.bytes_in += len(data)
                            break

            buffer += data
            while b'\n' in buffer:
                lb, buffer = buffer.split(b'\n', 1)
                line = lb.decode('utf-8', errors='replace').strip()
                if not line:
                    continue

                # диагностика: строки
                with multi_store.lock:
                    if self.name == "RX":
                        multi_store.rx_lines_in += 1
                    else:
                        for s in multi_store.tx_stores.values():
                            if s.port == self.port:
                                s.lines_in += 1
                                break

                try:
                    self.on_line(line)
                except Exception as e:
                    # Не проглатываем ошибку молча: показываем её в консоли
                    # и на панели Debug, но продолжаем читать следующие строки.
                    msg = f"{self.name} обработка строки: {type(e).__name__}: {e}"
                    print(f"[{self.name}] parse error: {msg}")
                    with multi_store.lock:
                        multi_store.error = msg

        try:
            if self.ser:
                self.ser.close()
        except Exception:
            pass

    def stop(self):
        self.stop_event.set()