import time
import backoff
import psycopg2
import httpx
import models
import json
from typing import Any
from utils import get_postges_connection
from state import State, JsonFileStorage
from settings import settings
from movie_index import movie_index_config


class BackoffQueryMixin:
    @backoff.on_exception(backoff.expo, exception=(psycopg2.OperationalError, psycopg2.InterfaceError))
    def _execute_query(self, conn: psycopg2.extensions.connection, query: str) -> list[Any]:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


class UUIDEscapingMixin:
    def escape_uuids(self, uuids: list[str]):
        return ', '.join(map(lambda uuid: f"'{uuid}'", uuids))


class PostgresProducer:
    tables = (
        'film_work',
        'person',
        'genre',
    )

    def __init__(self, state: State, conn: psycopg2.extensions.connection, extract_size: int) -> None:
        self.state = state
        self.conn = conn
        self.extract_size = extract_size

    def _get_query(
        self,
        table_name: str,
        modified_after: str,
    ):
        return f"""
            SELECT id, modified
            FROM content.{table_name}
            WHERE modified > '{modified_after}'
            ORDER BY modified;
        """

    @backoff.on_exception(backoff.expo, exception=(psycopg2.OperationalError, psycopg2.InterfaceError))
    def _iter_over_table(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)

        while True:
            rows = cursor.fetchmany(size=self.extract_size)
            if not rows:
                break
            yield [dict(row) for row in rows]

    def check_updates(self):
        for table in self.tables:
            producer_state = self.state.get_state('etl') or {}
            modified = producer_state.get('modified') or '1970-01-01 00:00:00.000 +0000'

            query = self._get_query(table_name=table, modified_after=modified)

            for entity_pack in self._iter_over_table(query):
                entity_ids = [entity['id'] for entity in entity_pack]
                modified_date = entity_pack[0]['modified']
                yield table, entity_ids, modified_date


class PostgresEnricher(BackoffQueryMixin, UUIDEscapingMixin):
    related_tables = {
        'person': 'person_film_work',
        'genre': 'genre_film_work',
        'film_work': None
    }

    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self.conn = conn

    def _get_query(
        self,
        table_name: str,
        ids: list[str],
    ):
        return f"""
            SELECT fw.id
            FROM content.film_work fw
            LEFT JOIN content.{self.related_tables[table_name]} rt ON rt.film_work_id = fw.id
            WHERE rt.{table_name}_id IN ({self.escape_uuids(ids)})
            ORDER BY modified;
        """

    def enrich(self, table: str, entity_ids: list[str]):
        if table == 'film_work':
            return entity_ids

        if table in ('person', 'genre'):
            rows_dict = self._execute_query(
                conn=self.conn,
                query=self._get_query(table_name=table, ids=entity_ids)
            )
            return [row['id'] for row in rows_dict]


class PostgresMerger(BackoffQueryMixin, UUIDEscapingMixin):
    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self.conn = conn

    def _get_query(self, film_work_ids: list[str]):
        return f"""
            SELECT
                fw.id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.created,
                fw.modified,
                pfw.role as person_role,
                p.id as person_id,
                p.full_name as person_full_name,
                g.name as genre_name
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN ({self.escape_uuids(film_work_ids)});
        """

    def _collect_genre_names(self, entities: list[dict]):
        genre_names = [
            entity['genre_name']
            for entity in entities
        ]
        return list(set(genre_names))

    def _collect_persons(self, entities: list[dict]):
        return [
            {
                'person_id': entity['person_id'],
                'person_role': entity['person_role'],
                'person_full_name': entity['person_full_name'],

            }
            for entity in entities
        ]

    def merge(self, data) -> list[dict]:
        res = []
        raw_data = self._execute_query(
            conn=self.conn,
            query=self._get_query(data)
        )

        data_by_id: dict[str, list[dict]] = {}

        for row in raw_data:
            if not data_by_id.get(row['id']):
                data_by_id[row['id']] = [row]
                continue
            data_by_id[row['id']].append(row)

        for entity_id, entity_data in data_by_id.items():
            res.append({
                'id': entity_id,
                'title': entity_data[0]['title'],
                'description': entity_data[0]['description'],
                'rating': entity_data[0]['rating'],
                'type': entity_data[0]['type'],
                'created': entity_data[0]['created'],
                'modified': entity_data[0]['modified'],
                'genres': self._collect_genre_names(entity_data),
                'persons': self._collect_persons(entity_data)
            })

        return res


class Transformer:
    model = models.Movie

    def _transform_persons(self, persons: list[dict]):
        processed_person_ids = set()
        res = {
            'directors_names': [],
            'actors_names': [],
            'writers_names': [],
            'directors': [],
            'actors': [],
            'writers': [],
        }

        for person in persons:
            person_id = person['person_id']
            person_role = person['person_role']
            person_full_name = person['person_full_name']

            if person_id in processed_person_ids:
                continue

            if person_role == 'actor':
                res['actors'].append({
                    'id': person_id,
                    'name': person_full_name
                })
                res['actors_names'].append(person_full_name)
            if person_role == 'director':
                res['directors'].append({
                    'id': person_id,
                    'name': person_full_name
                })
                res['directors_names'].append(person_full_name)
            if person_role == 'writer':
                res['writers'].append({
                    'id': person_id,
                    'name': person_full_name
                })
                res['writers_names'].append(person_full_name)
            processed_person_ids.add(person_id)

        return res

    def transform(self, data):
        res = []

        for item in data:
            transformed_persons = self._transform_persons(item['persons'])

            model = self.model(
                id=item['id'],
                imdb_rating=item['rating'],
                title=item['title'],
                description=item['description'],
                genres=item['genres'],
                directors_names=transformed_persons['directors_names'],
                actors_names=transformed_persons['actors_names'],
                writers_names=transformed_persons['writers_names'],
                actors=transformed_persons['actors'],
                directors=transformed_persons['directors'],
                writers=transformed_persons['writers']
            )

            res.append(model)

        return res


class ElasticsearchLoader:
    def __init__(self, service_url: str, index: str) -> None:
        self.client = httpx.Client()
        self.service_url = service_url
        self.index_name = index

    def _check_index_exists(self):
        response = self.client.head(f"{self.service_url}/{self.index_name}")
        return response.status_code == 200

    @backoff.on_exception(backoff.expo, exception=(httpx.ConnectError, httpx.ConnectTimeout))
    def _create_index(self):
        self.client.put(f"{self.service_url}/{self.index_name}", json=movie_index_config)

    @backoff.on_exception(backoff.expo, exception=(httpx.ConnectError, httpx.ConnectTimeout))
    def upload(self, movies: list[models.Movie]):
        if not self._check_index_exists():
            self._create_index()

        request_content = []

        for movie in movies:
            request_content.append(json.dumps({
                'index': {
                    '_index': self.index_name,
                    '_id': movie.id
                }
            }))
            request_content.append(movie.model_dump_json())

        request_content = '\n'.join(request_content)
        request_content += '\n'

        self.client.post(
            f'{self.service_url}/_bulk',
            headers={'Content-Type': 'application/x-ndjson'},
            content=request_content
        )


def main():
    config = settings.model_dump()
    conn = get_postges_connection(str(config['postgres']['dsn']))
    state = State(storage=JsonFileStorage(file_path=config['storage']['file_path']))
    producer = PostgresProducer(state=state, conn=conn, extract_size=config['etl']['extract_size'])
    enricher = PostgresEnricher(conn=conn)
    merger = PostgresMerger(conn=conn)
    transformer = Transformer()
    loader = ElasticsearchLoader(service_url=str(config['elastic']['url']), index=config['elastic']['index'])

    while True:
        updates = producer.check_updates()

        for updated_table, updated_entities, modified in updates:
            enriched_entities = enricher.enrich(table=updated_table, entity_ids=updated_entities)
            merged_entities = merger.merge(enriched_entities)
            transformed_entities = transformer.transform(merged_entities)
            loader.upload(transformed_entities)
            state.set_state(key='etl', value={'modified': modified.strftime('%Y-%m-%d %H:%M:%S.%f %z')})
            time.sleep(config['etl']['iteration_sleep_time'])
            print('sleeping over ')

        print('sleeping over checking')
        time.sleep(config['etl']['checking_sleep_time'])


if __name__ == '__main__':
    main()
