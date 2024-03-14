import asyncio
import functools

from motor.motor_asyncio import AsyncIOMotorClient
from motor.motor_asyncio import AsyncIOMotorDatabase


class ConfigHandler:
    _mongo_dsn: str = None
    _client: AsyncIOMotorClient = None
    _database: AsyncIOMotorDatabase = None
    _service_name: str = None

    def __init__(self, mongo_dsn: str, service_name: str):
        self._mongo_dsn = mongo_dsn
        self._service_name = service_name

    async def connect(self) -> None:
        self._client = AsyncIOMotorClient(self._mongo_dsn)
        self._database = self._client["services_configs"]

    async def config(self) -> dict:
        collection = self._database["services_configs"]
        result = await collection.find_one({"service": self._service_name})
        return result
