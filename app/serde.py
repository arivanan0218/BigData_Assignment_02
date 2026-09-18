"""Avro encoding using the reference `avro` library and the Confluent wire format:

    byte 0      magic byte, always 0x00
    bytes 1-4   schema id (big-endian int32) as assigned by the Schema Registry
    bytes 5..   Avro binary encoding of the record

The consumer reads the id, looks the schema up (once, then cached) and decodes.
"""
import io
import struct

import avro.io
import avro.schema

from .registry import SchemaRegistry

MAGIC = b"\x00"


class WireCodec:
    def __init__(self, registry: SchemaRegistry):
        self.registry = registry
        self._writer = None
        self._schema_id = None
        self._readers: dict = {}

    async def prepare_writer(self, subject: str, schema_json: str) -> int:
        self._schema_id = await self.registry.register(subject, schema_json)
        self._writer = avro.io.DatumWriter(avro.schema.parse(schema_json))
        return self._schema_id

    def encode(self, record: dict) -> bytes:
        buf = io.BytesIO()
        self._writer.write(record, avro.io.BinaryEncoder(buf))
        return MAGIC + struct.pack(">I", self._schema_id) + buf.getvalue()

    async def decode(self, payload: bytes) -> dict:
        if not payload or payload[0:1] != MAGIC:
            raise ValueError("not a schema-registry framed message")
        schema_id = struct.unpack(">I", payload[1:5])[0]
        if schema_id not in self._readers:
            schema = avro.schema.parse(await self.registry.fetch(schema_id))
            self._readers[schema_id] = avro.io.DatumReader(schema, schema)
        return self._readers[schema_id].read(avro.io.BinaryDecoder(io.BytesIO(payload[5:])))
