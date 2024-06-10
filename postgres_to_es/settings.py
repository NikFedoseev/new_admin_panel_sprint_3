import pydantic
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv('./.env')


class StorageSettings(pydantic.BaseModel):
    file_path: str


class PostgresSettings(pydantic.BaseModel):
    dsn: pydantic.PostgresDsn


class ElasticSettings(pydantic.BaseModel):
    url: pydantic.HttpUrl
    index: str


class ETLSettings(pydantic.BaseModel):
    start_time: str
    extract_size: int
    checking_sleep_time: float
    iteration_sleep_time: float


class Settings(BaseSettings):
    postgres: PostgresSettings
    elastic: ElasticSettings
    storage: StorageSettings
    etl: ETLSettings

    class Config:
        env_nested_delimiter = '__'
        env_file = './.env'
        env_file_encoding = 'utf-8'


settings = Settings()
