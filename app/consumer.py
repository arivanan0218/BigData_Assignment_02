"""Async consumer with tiered retry topics and a dead-letter topic.

    python -m app.consumer          # dashboard on http://127.0.0.1:8080

Coroutines running concurrently on one event loop:
  * consume(orders)             – main path
  * consume(orders.retry.5s)    – waits until each record is due, then re-processes
  * consume(orders.retry.15s)   – idem
  * consume(orders.retry.30s)   – idem; a failure here goes to the dead-letter topic
  * tail_dead_letters()         – mirrors the DLQ into the dashboard
  * web server
"""
import asyncio
import logging
import time

from aiohttp import web
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from . import settings
from .metrics import Metrics
from .registry import SchemaRegistry
from .serde import WireCodec
from .warehouse import RejectedOrder, WarehouseUnavailable, write_order
from .web import make_app

logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03d [%(name)s] %(message)s", datefmt="%H:%M:%S")
logging.getLogger("aiokafka").setLevel(logging.WARNING)
log = logging.getLogger("consumer")

ALL_TOPICS = [settings.TOPIC_ORDERS, *[t for t, _ in settings.RETRY_TIERS], settings.TOPIC_DLQ]


async def create_topics() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.BROKER)
    await admin.start()
    try:
        for name in ALL_TOPICS:
            try:
                await admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])
                log.info("created topic %s", name)
            except TopicAlreadyExistsError:
                pass
    finally:
        await admin.close()


def hdr(record, name: str, default: str = "") -> str:
    return next((v.decode() for k, v in record.headers if k == name), default)


class Pipeline:
    def __init__(self):
        self.metrics = Metrics()
        self.codec = WireCodec(SchemaRegistry())
        self.producer = None   # aiokafka clients must be created inside the running event loop (see run())

    async def forward(self, record, topic: str, extra: dict) -> None:
        headers = [(k, str(v).encode()) for k, v in extra.items()]
        await self.producer.send_and_wait(topic, key=record.key, value=record.value, headers=headers)

    async def process(self, record, tier_index: int) -> None:
        """tier_index = -1 for the main topic, 0..N-1 for retry tiers."""
        order = await self.codec.decode(record.value)
        attempt = tier_index + 2  # main topic = attempt 1
        if tier_index < 0:
            self.metrics.consumed_main()
        try:
            await write_order(order)
        except RejectedOrder as exc:
            await self.forward(record, settings.TOPIC_DLQ, {"reason": str(exc), "kind": "permanent",
                                                         "attempts": attempt, "failed_at": time.strftime("%H:%M:%S")})
            self.metrics.dead_letter(order, f"rejected: {exc}")
            log.error("✗ %s dead-lettered (%s)", order["orderId"], exc)
        except WarehouseUnavailable as exc:
            next_tier = tier_index + 1
            if next_tier < len(settings.RETRY_TIERS):
                topic, delay = settings.RETRY_TIERS[next_tier]
                await self.forward(record, topic, {"due_at": time.time() + delay, "attempt": attempt,
                                                   "error": str(exc)})
                self.metrics.retry(order, topic, delay, str(exc))
                log.warning("↻ %s attempt %d failed (%s) → %s", order["orderId"], attempt, exc, topic)
            else:
                await self.forward(record, settings.TOPIC_DLQ, {"reason": f"{exc} – still failing after {attempt} attempts",
                                                             "kind": "retries-exhausted", "attempts": attempt,
                                                             "failed_at": time.strftime("%H:%M:%S")})
                self.metrics.dead_letter(order, f"retries exhausted after {attempt} attempts")
                log.error("✗ %s dead-lettered after %d attempts", order["orderId"], attempt)
        else:
            self.metrics.ok(order, record.topic)
            log.info("✓ %s %-12s %8.2f  attempt %d  avg=%.2f", order["orderId"], order["product"], order["price"],
                     attempt, self.metrics.average)

    async def consume(self, topic: str, tier_index: int) -> None:
        consumer = AIOKafkaConsumer(topic, bootstrap_servers=settings.BROKER,
                                    group_id=f"{settings.GROUP_PREFIX}-{topic}",
                                    enable_auto_commit=False, auto_offset_reset="earliest")
        await consumer.start()
        log.info("listening on %s", topic)
        try:
            async for record in consumer:
                if tier_index >= 0:
                    wait = float(hdr(record, "due_at", "0")) - time.time()
                    if wait > 0:
                        await asyncio.sleep(wait)   # only this coroutine waits; the others keep going
                await self.process(record, tier_index)
                await consumer.commit()
        finally:
            await consumer.stop()

    async def tail_dead_letters(self) -> None:
        consumer = AIOKafkaConsumer(settings.TOPIC_DLQ, bootstrap_servers=settings.BROKER,
                                    group_id=f"{settings.GROUP_PREFIX}-dlq-tail", auto_offset_reset="earliest")
        await consumer.start()
        try:
            async for record in consumer:
                order = await self.codec.decode(record.value)
                headers = {k: v.decode() for k, v in record.headers}
                self.metrics.dlq_seen(order, headers, record.offset)
        finally:
            await consumer.stop()

    async def serve_web(self) -> None:
        runner = web.AppRunner(make_app(self.metrics))
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", settings.WEB_PORT).start()
        log.info("dashboard: http://127.0.0.1:%d", settings.WEB_PORT)
        await asyncio.Event().wait()

    async def run(self) -> None:
        await create_topics()
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.BROKER, acks="all")
        await self.producer.start()
        try:
            await asyncio.gather(
                self.serve_web(),
                self.tail_dead_letters(),
                self.consume(settings.TOPIC_ORDERS, -1),
                *[self.consume(topic, i) for i, (topic, _) in enumerate(settings.RETRY_TIERS)],
            )
        finally:
            await self.producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(Pipeline().run())
    except KeyboardInterrupt:
        pass
