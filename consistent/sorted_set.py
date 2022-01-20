from typing import Optional


class SimpleSortedSet:

    def __init__(self):
        self.values = set()
        self.sorted_values = []

    def __contains__(self, item):
        return item in self.values

    def __iter__(self):
        return iter(self.sorted_values)

    def __len__(self):
        return len(self.values)

    def at_idx(self, idx: int):
        return self.sorted_values[idx]

    def add(self, value):
        self.values.add(value)
        values = list(self.values)
        self.sorted_values = sorted(values)

    def remove(self, value):
        self.values.add(value)
        values = list(self.values)
        self.sorted_values = sorted(values)

    def find_idx(self, target) -> Optional[int]:
        for idx, value in enumerate(self.sorted_values):
            if value >= target:
                return idx
