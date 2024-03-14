import asyncio
import uuid
from datetime import datetime

from app.config import app_config
from app.rabbitmq import RPCHandler
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("aiormq").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.ERROR)


async def worker(request: dict):
    logging.info(f"Worker received request: {request}")
    logging.info(f"Doing job")
    start_dt = str(datetime.utcnow())
    await asyncio.sleep(5)
    return {
        "status": "success",
        "data": str(uuid.uuid4()),
        "request": request,
        "start_at": start_dt,
        "end_at": str(datetime.utcnow())

    }


async def main() -> None:
    rpc_handler = RPCHandler(amqp_dsn=str(app_config.amqp_dsn), service_name=app_config.service_name, consumer_callback=worker)
    await asyncio.create_task(rpc_handler.connect())

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(main())
    loop.run_forever()
