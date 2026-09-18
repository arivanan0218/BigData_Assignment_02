"""In-memory metrics shared by all coroutines (single event loop, so no locking needed)."""
import time
from collections import deque

from . import settings


class Metrics:
    def __init__(self):
        self.started = time.time()
        self.count = 0
        self.total = 0.0
        self.window = deque(maxlen=settings.WINDOW_SIZE)
        self.products: dict = {}
        self.tier_hits = {name: 0 for name, _ in settings.RETRY_TIERS}
        self.main_seen = 0
        self.recovered = 0
        self.dead = 0
        self.events = deque(maxlen=30)
        self.dlq = deque(maxlen=20)

    def consumed_main(self) -> None:
        self.main_seen += 1

    def ok(self, order: dict, source_topic: str) -> None:
        price = order["price"]
        self.count += 1
        self.total += price
        self.window.append(price)
        p = self.products.setdefault(order["product"], {"n": 0, "sum": 0.0})
        p["n"] += 1
        p["sum"] += price
        if source_topic != settings.TOPIC_ORDERS:
            self.recovered += 1
        self.event("ok", order, f"stored via {source_topic} · avg {self.average:.2f} · window avg {self.window_average:.2f}")

    def retry(self, order: dict, tier: str, delay: int, reason: str) -> None:
        self.tier_hits[tier] += 1
        self.event("retry", order, f"{reason} → {tier} (in {delay}s)")

    def dead_letter(self, order: dict, reason: str) -> None:
        self.dead += 1
        self.event("dead", order, reason)

    def dlq_seen(self, order: dict, headers: dict, offset: int) -> None:
        self.dlq.appendleft({"offset": offset, **order, "price": round(order["price"], 2), **headers})

    def event(self, kind: str, order: dict, text: str) -> None:
        self.events.appendleft({"t": time.strftime("%H:%M:%S"), "kind": kind,
                                "id": order.get("orderId", "?"), "product": order.get("product", "?"),
                                "price": round(order.get("price", 0.0), 2), "text": text})

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0

    @property
    def window_average(self) -> float:
        return sum(self.window) / len(self.window) if self.window else 0.0

    def to_dict(self) -> dict:
        return {
            "uptime": int(time.time() - self.started),
            "count": self.count,
            "average": round(self.average, 2),
            "window_size": settings.WINDOW_SIZE,
            "window_average": round(self.window_average, 2),
            "window": [round(v, 2) for v in self.window],
            "products": {k: {"n": v["n"], "avg": round(v["sum"] / v["n"], 2)} for k, v in sorted(self.products.items())},
            "tiers": [{"topic": name, "delay": delay, "hits": self.tier_hits[name]} for name, delay in settings.RETRY_TIERS],
            "main_seen": self.main_seen,
            "recovered": self.recovered,
            "dead": self.dead,
            "events": list(self.events),
            "dlq": list(self.dlq),
        }
