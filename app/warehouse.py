"""Simulated warehouse database. Writing an order can fail in two ways."""
import asyncio
import random

from . import settings


class WarehouseUnavailable(Exception):
    """temporary – lock timeout / connection reset; retry later"""


class RejectedOrder(Exception):
    """permanent – violates a constraint; will fail forever"""


async def write_order(order: dict) -> None:
    await asyncio.sleep(0.05)  # pretend I/O
    if order["product"].lower().startswith("unknown"):
        raise RejectedOrder(f"product '{order['product']}' is not in the warehouse catalogue")
    if order["price"] <= 0:
        raise RejectedOrder(f"price must be positive, got {order['price']:.2f}")
    if random.random() < settings.UNAVAILABLE_RATE:
        raise WarehouseUnavailable("warehouse db lock timeout")
