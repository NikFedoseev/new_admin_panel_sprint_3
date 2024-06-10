import backoff
import psycopg2
import psycopg2.extras


@backoff.on_exception(backoff.expo, exception=(psycopg2.OperationalError, psycopg2.InterfaceError))
def get_postges_connection(dsn: str):
    conn = psycopg2.connect(dsn=dsn, cursor_factory=psycopg2.extras.DictCursor)
    return conn
