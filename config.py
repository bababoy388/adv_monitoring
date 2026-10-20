"""
Конфигурация приложения.
Все изменяемые параметры — здесь.
"""

# ─── Serial ───
BAUD_RATE = 115200

# ─── Тайминги синхронизации ───
TX_SLOT_WIDTH_MS = 900

# ─── UI / Web ───
REFRESH_HZ = 4
MAX_ROWS = 200
RAW_LINES = 100
SYSTEM_CYCLE_MS = 10000
SYNC_TARGET_DRIFT_MS = 0