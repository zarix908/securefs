import dataclasses
from enum import Enum


class NodeType(Enum):
    FILE = 0
    DIRECTORY = 1
    UNKNOWN = 2


@dataclasses.dataclass
class Signal:
    path: str
    type: NodeType
