from dataclasses import dataclass, asdict, fields
from typing import List
import logging
import json

logger = logging.getLogger(__name__)


@dataclass
class Record:
    @classmethod
    def from_data(cls, data: dict):
        attributes = {}
        for field in fields(cls):
            attributes[field.name] = data[field.name]
        record = cls(**attributes)
        return record

    def dumps(self):
        return json.dumps(asdict(self))

    def to_dict(self):
        return asdict(self)

    def get(self, key: str):
        return self.to_dict().get(key)

    @classmethod
    def fields_names(cls) -> List[str]:
        return [field.name for field in fields(cls)]

