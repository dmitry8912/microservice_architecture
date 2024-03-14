import json
import uuid
from asyncio import Future
from typing import Dict
from app.config import app_config

import aio_pika
import logging


__all__ = ["rpc_handler"]


class RPCHandler:
    _connection: aio_pika.abc.AbstractRobustConnection = None
    _channel: aio_pika.abc.AbstractChannel = None
    _response_queue: aio_pika.abc.AbstractQueue = None

    _amqp_dsn: str = None

    _futures: Dict[str, Future] = {}

    def __init__(self, amqp_dsn: str):
        self._amqp_dsn = amqp_dsn

    async def connect(self) -> None:
        logging.info("AMQP connecting")
        self._connection = await aio_pika.connect_robust(self._amqp_dsn)
        logging.info("AMQP created connecton")
        self._channel = await self._connection.channel()
        logging.info("AMQP created channel")
        self._response_queue = await self._channel.declare_queue("api_responses", durable=True)
        logging.info("AMQP created response queue")
        await self._response_queue.consume(self._consume)

    async def _consume(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        logging.info(f"AMQP consumer incoming message {message.message_id}")
        correlation_id = message.correlation_id
        future = self._futures.get(correlation_id, None)
        if future:
            future.set_result(json.loads(message.body.decode("utf-8")))
        logging.info(f"AMQP consumer finished message {message.message_id}")
        await message.ack()

    async def _request(self, *, to: str, payload: dict):
        request_id = str(uuid.uuid4())
        logging.info(f"AMQP send request {request_id} to {to}")
        self._futures[request_id] = Future()
        message = aio_pika.message.Message(
            body=json.dumps(payload).encode("utf-8"),
            correlation_id=request_id,
            reply_to=self._response_queue.name
        )

        await self._channel.default_exchange.publish(message, routing_key=to)

        return await self._futures[request_id]

    async def make_book_cover(self, book_name: str):
        return await self._request(to="cover_creator_requests", payload={"book_name": book_name})

    async def make_book_pdf_layout(self, book_name: str):
        return await self._request(to="layout_creator_requests", payload={"book_name": book_name})

    async def make_book_description(self, book_name: str):
        return await self._request(to="description_creator_requests", payload={"book_name": book_name})

    async def make_book_publish(self, book_name: str):
        return await self._request(to="book_publisher_requests", payload={"book_name": book_name})


rpc_handler = RPCHandler(str(app_config.amqp_dsn))
