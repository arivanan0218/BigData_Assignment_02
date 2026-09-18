"""Async order producer.

    python -m app.producer [--interval 1.0] [--bad-every 8]

Registers schemas/order.avsc with the registry, then publishes orders framed in the
Confluent wire format (magic byte + schema id + Avro binary).
"""
import argparse
import asyncio
import logging
import random

from aiokafka import AIOKafkaProducer

from . import settings
from .registry import SchemaRegistry, load_schema_json
from .serde import WireCodec

logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03d [%(name)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("producer")

CATALOGUE = ["Item1", "Item2", "Item3", "Item4", "Item5", "Item6"]


def random_order(n: int, poison: bool) -> dict:
    order_id = str(3000 + n)
    if poison and (n // 8) % 2:
        return {"orderId": order_id, "product": "Unknown-SKU", "price": round(random.uniform(10, 90), 2)}
    if poison:
        return {"orderId": order_id, "product": random.choice(CATALOGUE), "price": 0.0}
    # prices cluster around 120 with a long tail
    price = max(1.0, random.gauss(120, 60))
    return {"orderId": order_id, "product": random.choice(CATALOGUE), "price": round(price, 2)}


async def main(interval: float, bad_every: int) -> None:
    codec = WireCodec(SchemaRegistry())
    schema_id = await codec.prepare_writer(f"{settings.TOPIC_ORDERS}-value", load_schema_json())
    log.info("schema registered under '%s-value' with id %d", settings.TOPIC_ORDERS, schema_id)

    producer = AIOKafkaProducer(bootstrap_servers=settings.BROKER, acks="all")
    await producer.start()
    n = 0
    try:
        while True:
            n += 1
            order = random_order(n, bad_every > 0 and n % bad_every == 0)
            meta = await producer.send_and_wait(settings.TOPIC_ORDERS, key=order["orderId"].encode(), value=codec.encode(order))
            log.info("→ %s  %-12s %8.2f   (%s p%d o%d)", order["orderId"], order["product"], order["price"],
                     meta.topic, meta.partition, meta.offset)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        await producer.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between orders")
    ap.add_argument("--bad-every", type=int, default=8, help="every Nth order is un-storable (0 = never)")
    args = ap.parse_args()
    try:
        asyncio.run(main(args.interval, args.bad_every))
    except KeyboardInterrupt:
        pass
