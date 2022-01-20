from typing import Protocol

import xxhash


class Hasher(Protocol):

    def hash_str(self, value: str) -> int:
        ...


class XXHasher(Hasher):

    def hash_str(self, value: str) -> int:
        x = xxhash.xxh64()
        x.update(value.encode())
        return x.intdigest()