import os
from pathlib import Path

BROKER = os.getenv("BROKER", "localhost:9094")
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:8084")
SCHEMA_FILE = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"

TOPIC_ORDERS = "orders"
TOPIC_DLQ = "orders.dead-letter"
# Tiered retry topics: each tier delays re-processing by a fixed amount.
RETRY_TIERS = [
    ("orders.retry.5s", 5),
    ("orders.retry.15s", 15),
    ("orders.retry.30s", 30),
]

GROUP_PREFIX = "warehouse-4414"
UNAVAILABLE_RATE = float(os.getenv("UNAVAILABLE_RATE", "0.30"))
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "20"))

WEB_PORT = int(os.getenv("WEB_PORT", "8080"))
