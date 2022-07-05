import os
import pickle
from typing import Any, List, Protocol


class PersistencyProvider(Protocol):
    def list_keys(self, path: str) -> List[str]:
        pass

    def get_item(self, key: str) -> Any:
        pass

    def delete_item(self, key: str) -> None:
        pass

    def upsert_item(self, key: str, item: Any) -> None:
        pass


class PathPersistencyProvider(PersistencyProvider):
    def __init__(self, base_path: str):
        if base_path.endswith("/"):
            raise Exception("Base path shouldn't end with /")
        self.base_path = base_path

    def _get_path(self, path: str) -> str:
        return f"{self.base_path}/{path}"

    def list_keys(self, path: str) -> List[str]:
        return os.listdir(self._get_path(path))

    def get_item(self, key: str) -> Any:
        key = self._get_path(key)
        with open(key, "rb") as f:
            return pickle.load(f)

    def delete_item(self, key: str) -> None:
        key = self._get_path(key)
        os.remove(key)

    def upsert_item(self, key: str, item: Any) -> None:
        key = self._get_path(key)
        with open(key, "wb") as f:
            return pickle.dump(item, f)
