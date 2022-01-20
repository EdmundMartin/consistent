from typing import Protocol


class Node(Protocol):

    def name(self) -> str:
        ...
