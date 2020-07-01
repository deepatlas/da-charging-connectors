import json
from abc import ABC, abstractmethod
from typing import List
from charging_stations.helpers import default, object_hook


class Connector(ABC):
    @property
    def __data_source__(self):
        raise NotImplementedError

    @property
    def raw_data(self):
        raise NotImplementedError

    @property
    def processed_data(self):
        raise NotImplementedError

    @abstractmethod
    def get_data(self, to_disk: bool):
        pass

    @abstractmethod
    def process(self, to_disk: bool):
        pass

    @abstractmethod
    def load(self, is_processed: bool, is_test: bool):
        pass

    @staticmethod
    def _save(file_path: str, content_list: List[any]):
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(
                content_list, f, ensure_ascii=False, indent=4, default=default,
            )

    @staticmethod
    def _load(file_path: str) -> List[any]:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f, object_hook=object_hook)
