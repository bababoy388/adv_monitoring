"""Потоковая запись исходных строк последовательных портов в CSV."""
import csv
import threading
from datetime import datetime
from pathlib import Path


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
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.session_dir = Path('data') / f'session_{stamp}'
            self.session_dir.mkdir(parents=True, exist_ok=False)
            self.path = self.session_dir / 'serial_log.csv'
            self._file = self.path.open('w', newline='', encoding='utf-8-sig', buffering=1)
            self._writer = csv.writer(self._file, delimiter=';', lineterminator='\n')
            self._writer.writerow(['timestamp_local', 'elapsed_s', 'source', 'port', 'line'])
            self._file.flush()
            return str(self.path.resolve())

    def write_line(self, source, port, line, elapsed_s):
        with self._lock:
            if not self._writer or not self._file:
                return
            self._writer.writerow([
                datetime.now().astimezone().isoformat(timespec='milliseconds'),
                f'{elapsed_s:.3f}', source, port, line
            ])
            # Сразу сбрасываем строку на диск: данные не ждут ручного экспорта.
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
