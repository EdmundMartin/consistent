from typing import Protocol

import xxhash


class Hasher(Protocol):

    def hash_str(self, value: str) -> int:
        ...

    def hash_int(self, values: int) -> int:
        ...


class XXHasher(Hasher):

    def hash_str(self, value: str) -> int:
        x = xxhash.xxh64()
        x.update(value.encode())
        return x.intdigest()

    def hash_int(self, value: int) -> int:
        x = xxhash.xxh64()
        x.update(value.to_bytes(8, "little"))
        return x.intdigest()
