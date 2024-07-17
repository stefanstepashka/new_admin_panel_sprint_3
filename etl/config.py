from pydantic import BaseModel
from pydantic_settings import BaseSettings

class PostgresConfig(BaseSettings):
    user: str
    password: str
    database: str
    host: str
    port: int

    class Config:
        env_prefix = 'PG_'


class ElasticsearchConfig(BaseSettings):
    host: str
    port: int

    class Config:
        env_prefix = 'ES_'



