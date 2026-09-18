"""Minimal async client for the Confluent-compatible Schema Registry REST API
(served here by Redpanda). Only two calls are needed:

    POST /subjects/{subject}/versions   -> register a schema, get its id
    GET  /schemas/ids/{id}              -> fetch a schema by id
"""
import json

import aiohttp

from . import settings


class SchemaRegistry:
    def __init__(self, base_url: str = settings.REGISTRY_URL):
        self.base_url = base_url.rstrip("/")
        self._by_id: dict = {}

    async def register(self, subject: str, schema_json: str) -> int:
        async with aiohttp.ClientSession() as http:
            async with http.post(
                f"{self.base_url}/subjects/{subject}/versions",
                json={"schema": schema_json, "schemaType": "AVRO"},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            ) as resp:
                resp.raise_for_status()
                schema_id = (await resp.json())["id"]
        self._by_id[schema_id] = schema_json
        return schema_id

    async def fetch(self, schema_id: int) -> str:
        if schema_id not in self._by_id:
            async with aiohttp.ClientSession() as http:
                async with http.get(f"{self.base_url}/schemas/ids/{schema_id}") as resp:
                    resp.raise_for_status()
                    self._by_id[schema_id] = (await resp.json())["schema"]
        return self._by_id[schema_id]


def load_schema_json() -> str:
    # normalise whitespace so the registry sees a canonical string
    return json.dumps(json.loads(settings.SCHEMA_FILE.read_text()), separators=(",", ":"))
