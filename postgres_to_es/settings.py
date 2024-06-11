from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv('./.env')


class Settings(BaseSettings):
    postgres_db_host: str = Field(alias='POSTGRES_DB_HOST')
    postgres_db_port: str = Field(alias='POSTGRES_DB_PORT')
    postgres_db_user: str = Field(alias='POSTGRES_DB_USER')
    postgres_db_password: str = Field(alias='POSTGRES_DB_PASSWORD')
    postgres_db_name: str = Field(alias='POSTGRES_DB_NAME')
    storage_file_path: str = Field(alias='STORAGE_FILE_PATH')
    elastic_host: str = Field(alias='ELASTIC_HOST')
    elastic_port: str = Field(alias='ELASTIC_PORT')
    elastic_index: str = Field(alias='ELASTIC_INDEX')
    etl_start_time: str = Field(alias='ETL_START_TIME')
    etl_extract_size: int = Field(alias='ETL_EXTRACT_SIZE')
    etl_checking_updates_sleep_time: float = Field(alias='ETL_CHECKING_UPDATES_SLEEP_TIME')
    etl_iteration_sleep_time: float = Field(alias='ETL_ITERATION_SLEEP_TIME')


settings = Settings().model_dump()  # type: ignore
