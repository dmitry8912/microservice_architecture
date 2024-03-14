import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from app.rabbitmq import rpc_handler
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("aiormq").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.ERROR)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.create_task(rpc_handler.connect())
    yield

app = FastAPI(lifespan=lifespan)


@app.get('/')
async def make_book():
    logging.info("Make book request")
    book_cover = await rpc_handler.make_book_cover(book_name="Война и мир")
    logging.info("Created book cover")
    layout = await rpc_handler.make_book_pdf_layout(book_name="Война и мир")
    logging.info("Created layout")
    description = await rpc_handler.make_book_description(book_name="Война и мир")
    logging.info("Created description")
    publish = await rpc_handler.make_book_publish(book_name="Война и мир")
    logging.info("Book published")
    return {
        "book_cover": book_cover,
        "layout": layout,
        "description": description,
        "publish": publish
    }


@app.get('/parallel')
async def make_book_parallel():
    logging.info("Make book request")

    book = {}

    book_cover = await rpc_handler.make_book_cover(book_name="Война и мир")

    book["book_cover"] = book_cover

    parallel_tasks = [rpc_handler.make_book_pdf_layout(book_name="Война и мир"),
                      rpc_handler.make_book_description(book_name="Война и мир")]

    for job in asyncio.as_completed(parallel_tasks):
        result = await job
        logging.info(f"Result of parallel job: {result}")
        book[result["service"]] = result

    logging.info("Created description")
    publish = await rpc_handler.make_book_publish(book_name="Война и мир")

    book["publish"] = publish

    logging.info("Book published")
    return book


@app.get('/choreo')
async def make_book_parallel():
    logging.info("Make book request")
    result = await rpc_handler.make_book_cover(book_name="Война и мир")
    return result
