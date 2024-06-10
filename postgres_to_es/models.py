from typing import Union
from pydantic import BaseModel


class PersonEntity(BaseModel):
    id: str
    name: str


class Movie(BaseModel):
    id: str
    imdb_rating: Union[float, None]
    title: str
    description: Union[str, None]
    genres: list[str]
    directors_names: list[str]
    actors_names: list[str]
    writers_names: list[str]
    directors: list[PersonEntity]
    actors: list[PersonEntity]
    writers: list[PersonEntity]
