# warehouse-4414 — async Kafka order pipeline (Avro · tiered retries · dead-letter topic)

Arivanan V. — EG/2021/4414 — EC 8203 Applied Big Data Engineering, Chapter 3 assignment

## Idea

Everything is `asyncio`. One process (`app.consumer`) runs, on a single event loop:

* a consumer for `orders`
* one consumer per **retry tier** – `orders.retry.5s`, `orders.retry.15s`, `orders.retry.30s`
* a consumer tailing `orders.dead-letter` for the dashboard
* an `aiohttp` web server on http://127.0.0.1:8080

A temporary failure moves the record to the next tier (with a `due_at` header); the tier
consumer sleeps until `due_at` and tries again. Because each tier is its own coroutine the
sleep never blocks the main topic. After the 30 s tier the order is dead-lettered. A
permanent failure is dead-lettered immediately.

```
orders ──► write_order() ──ok──► running average (all-time + last-20 window)
             │ WarehouseUnavailable
             ▼
      orders.retry.5s ──(5s)──► write_order() ──fail──► orders.retry.15s ──(15s)──► … ──► orders.retry.30s ──(30s)──► fail
             │ RejectedOrder (any stage)                                                                                 │
             └────────────────────────────────────────────► orders.dead-letter ◄────────────────────────────────────────┘
```

## Avro without a client wrapper

The producer uses the reference `avro` package (`DatumWriter` + `BinaryEncoder`) and frames
the bytes itself in the Confluent wire format:

```
0x00 | schema-id (4 bytes, big endian) | Avro binary
```

The schema id comes from **Redpanda's built-in Schema Registry** (`POST /subjects/orders-value/versions`).
The consumer reads the id from the first five bytes, fetches the schema once (`GET /schemas/ids/{id}`),
caches it, and decodes with `DatumReader`. `app/registry.py` and `app/serde.py` are ~60 lines together.

## Layout

```
4414/
├── docker-compose.yml      Redpanda (Kafka API on :9094, Schema Registry on :8084)
├── run.sh                  setup | up | down | producer | consumer | topics
├── requirements.txt        aiokafka, avro, aiohttp
├── schemas/order.avsc
└── app/
    ├── settings.py         broker, topics, tiers [(orders.retry.5s,5),(…15s,15),(…30s,30)], failure rate
    ├── registry.py         SchemaRegistry (register / fetch by id) over aiohttp
    ├── serde.py            WireCodec – encode/decode with magic byte + schema id
    ├── warehouse.py        write_order(): WarehouseUnavailable (temporary) / RejectedOrder (permanent)
    ├── metrics.py          running average, sliding window, per-product, tier counters, DLQ mirror
    ├── producer.py         python -m app.producer [--interval 1] [--bad-every 8]
    ├── consumer.py         Pipeline: main + tier consumers + DLQ tail + web, all on one loop
    └── web.py              aiohttp page + /api/state JSON (polled every second)
```

## Run it

```bash
cd 4414
./run.sh setup            # venv + requirements
./run.sh up               # start Redpanda (wait ~15 s; ./run.sh topics should answer)
./run.sh consumer         # terminal 1 → open http://127.0.0.1:8080
./run.sh producer         # terminal 2 (add --interval 0.5 for a faster stream)
```

`./run.sh down` removes the container and its data.

## Walk-through for the demo

| step | show |
|---|---|
| schema | `curl localhost:8084/subjects/orders-value/versions/1` – the registered Avro schema and its id; the producer logs `schema registered … with id 1` |
| wire format | `docker exec redpanda-4414 rpk topic consume orders -n 1 -f '%v\n'` – first byte is `\x00`, then the id |
| aggregation | "running average (all)" and "average of last 20" tiles, plus the bar strip of the last 20 prices and the per-product table |
| retries | ~30 % of warehouse writes fail temporarily: the event stream prints `RETRY … → orders.retry.5s (in 5s)`; 5 s later the same order shows up as `OK … (via orders.retry.5s)`. The tier boxes count how many orders passed through each tier |
| DLQ | every 8th order is un-storable (`Unknown-SKU` product or price `0.00`): it goes straight to `orders.dead-letter` with a `reason` header, and the bottom table – filled by a real consumer of that topic – lists it. Orders that fail on all three tiers (~2.7 % of them) also arrive there with `attempts = 4` |
| offsets | Ctrl+C the consumer, wait a few seconds, start it again: nothing is lost, it resumes from the committed offsets of every topic |

Knobs: `UNAVAILABLE_RATE=0.6 ./run.sh consumer` (more retries), `./run.sh producer --bad-every 4`.

## Notes

* Avro `float` is 32-bit; a price of `123.45` decodes as `123.44999694824219`. The UI rounds to cents.
* Each tier has its own consumer group so its offsets are committed independently.
* Redpanda was chosen because it ships the Schema Registry in the same container – one image, one port mapping per API.
