import json
from typing import Callable

import aio_pika
import logging


__all__ = ["RPCHandler"]


class RPCHandler:
    _connection: aio_pika.abc.AbstractRobustConnection = None
    _channel: aio_pika.abc.AbstractChannel = None
    _request_queue: aio_pika.abc.AbstractQueue = None

    _amqp_dsn: str = None
    _service_name: str = None
    _consumer_callback: Callable = None

    def __init__(self, amqp_dsn: str, service_name: str, consumer_callback: Callable):
        self._amqp_dsn = amqp_dsn
        self._service_name = service_name
        self._consumer_callback = consumer_callback

    async def connect(self) -> None:
        logging.info("AMQP connecting")
        self._connection = await aio_pika.connect_robust(self._amqp_dsn)
        logging.info("AMQP created connecton")
        self._channel = await self._connection.channel()
        logging.info("AMQP created channel")
        self._request_queue = await self._channel.declare_queue(f"{self._service_name}_requests", durable=True)
        logging.info("AMQP created response queue")
        await self._request_queue.consume(self._consume)

    async def _consume(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        logging.info(f"AMQP consumer incoming message {message.message_id} from {message.reply_to} with id {message.correlation_id}")
        result = await self._consumer_callback(json.loads(message.body.decode("utf-8")))
        await self._response(
            to=message.reply_to,
            correlation_id=message.correlation_id,
            payload={
                "service": self._service_name,
                "result": result
            }
        )
        logging.info(f"AMQP consumer finished message {message.message_id} from {message.reply_to} with id {message.correlation_id}")
        await message.ack()

    async def _response(self, *, to: str, correlation_id: str, payload: dict):
        logging.info(f"AMQP send response {correlation_id} to {to}")
        message = aio_pika.message.Message(
            body=json.dumps(payload).encode("utf-8"),
            correlation_id=correlation_id
        )

        await self._channel.default_exchange.publish(message, routing_key=to)
