from pydantic_settings import BaseSettings
from pydantic import AmqpDsn, MongoDsn

__all__ = ['app_config']


class Settings(BaseSettings):
    amqp_dsn: AmqpDsn = "amqp://guest:guest@127.0.0.1:5672/msa_otus"
    mongo_dsn: MongoDsn = "mongodb://root:root@mongo:27017/"
    service_name: str


app_config = Settings()
