import logging
import sys
import backoff
import psycopg2
import psycopg2.extras
from settings import settings


logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.setLevel(level=logging.INFO)
logger.addHandler(handler)


@backoff.on_exception(backoff.expo, exception=(psycopg2.OperationalError, psycopg2.InterfaceError), logger=logger)
def get_postges_connection():
    dsl = {
        'dbname': settings['postgres_db_name'],
        'user': settings['postgres_db_user'],
        'password': settings['postgres_db_password'],
        'host': settings['postgres_db_host'],
        'port': settings['postgres_db_port']
    }
    conn = psycopg2.connect(**dsl, cursor_factory=psycopg2.extras.DictCursor)
    return conn
