import abc
from typing import Any, Dict
import json


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: dict[str, Any]) -> None:
        ...

    @abc.abstractmethod
    def retreive_state(self) -> dict[str, Any]:
        ...


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        storage_data = self.retreive_state()
        with open(self.file_path, 'w') as f:
            f.write(json.dumps({
                **storage_data,
                **state
            }))

    def retreive_state(self) -> Dict[str, Any]:
        try:
            with open(self.file_path, 'r') as f:
                storage_data = f.read()
                if not storage_data:
                    return {}

                return json.loads(storage_data)
        except FileNotFoundError:
            return {}


class State:
    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any):
        self.storage.save_state({key: value})

    def get_state(self, key: str):
        storage_data = self.storage.retreive_state()
        return storage_data.get(key)
