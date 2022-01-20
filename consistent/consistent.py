from typing import Dict, List, Optional
from math import ceil

from consistent.config import Config
from consistent.node import Node
from consistent.sorted_set import SimpleSortedSet
from consistent.xxhasher import Hasher, XXHasher

import xxhash


class Consistent:

    def __init__(self, nodes: List[Node], configuration: Config,
                 hasher: Hasher =XXHasher()):
        self.config = configuration
        self.loads: Dict[str, float] = {}
        self.members: Dict[str, Node] = {}
        self.partitions: Dict[int, Node] = {}
        self.ring: Dict[int, Node] = {}
        self.sorted_set: SimpleSortedSet = SimpleSortedSet()
        self.hasher = hasher

        for node in nodes:
            self._add(node)

        if len(self.members) > 0:
            self._distribute_partitions()

    def _add(self, node: Node) -> None:
        idx = 0
        while idx < self.config.replication_factor:
            key = f"{node.name()}{idx}"
            x = xxhash.xxh64()
            x.update(key.encode())
            result = x.intdigest()
            self.ring[result] = node
            self.sorted_set.add(result)
            idx += 1
        self.members[node.name()] = node

    def add(self, node: Node):
        if node.name() in self.members:
            return
        self._add(node)
        self._distribute_partitions()

    def remove(self, node: Node):
        if node.name() not in self.members:
            return
        i = 0
        while i < self.config.replication_factor:
            key = f"{node.name()}{i}"
            result = self.hasher.hash_str(key)
            del self.ring[result]
            self.sorted_set.remove(result)
            i += 1
        del self.members[node.name()]
        self._distribute_partitions()

    def _distribute_partitions(self):
        loads: Dict[str, float] = {}
        partitions: Dict[int, Node] = {}
        idx = 0
        while idx < self.config.partition_count:
            x = xxhash.xxh64()
            x.update(idx.to_bytes(8, "little"))
            key = x.intdigest()
            key_idx = self.sorted_set.find_idx(key)
            if key_idx is None:
                key_idx = 0
            self._distribute_with_load(idx, key_idx, partitions, loads)
            idx += 1
        self.loads = loads
        self.partitions = partitions

    def _distribute_with_load(self, partition_id, idx, partitions, loads):
        average_load = self.average_load()
        count = 0
        while True:
            count += 1
            if count >= len(self.sorted_set):
                raise ValueError("Not enough room to distribute partitions")
            i = self.sorted_set.at_idx(idx)
            node = self.ring[i]
            load = loads.get(node.name(), 0)
            if load + 1 <= average_load:
                partitions[partition_id] = node
                if node.name() in loads:
                    loads[node.name()] += 1
                else:
                    loads[node.name()] = 1
                return
            idx += 1
            if idx >= len(self.sorted_set):
                idx = 0

    def average_load(self) -> float:
        avg_load = self.config.partition_count / len(self.members) * self.config.load
        return ceil(avg_load)

    def load_distribution(self) -> Dict[str, float]:
        copied_loads = self.loads.copy()
        return copied_loads

    def _get_closest_n(self, partition_id: int, count) -> List[Node]:

        result = []
        if count > len(self.members):
            raise ValueError(f"Not enough members for closest n: {count}")

        owner_key = None
        owner = self.get_partition_owner(partition_id)
        keys = []
        key_nodes: Dict[int, Node] = {}
        for name, node in self.members.items():
            key = self.hasher.hash_str(name)
            if name == owner.name():
                owner_key = key
            keys.append(key)
            key_nodes[key] = node
        keys.sort()

        idx = 0
        while idx < len(keys):
            if keys[idx] == owner_key:
                key = keys[idx]
                result.append(key_nodes[key])
                break
            idx += 1

        while len(result) < count:
            idx += 1
            if idx >= len(keys):
                idx = 0
            key = keys[idx]
            result.append(key_nodes[key])

        return result

    def find_partition_id(self, key: str) -> int:
        x = xxhash.xxh64()
        x.update(key.encode())
        result = x.intdigest()
        return result % self.config.partition_count

    def get_partition_owner(self, partition_id: int) -> Optional[Node]:
        if partition_id in self.partitions:
            value = self.partitions[partition_id]
            return value
        return None

    def locate_key(self, key: str) -> Node:
        partition_id = self.find_partition_id(key)
        return self.partitions[partition_id]

    def closest_n_for_key(self, key: str, count: int) -> List[Node]:
        partition_id = self.find_partition_id(key)
        return self._get_closest_n(partition_id, count)

