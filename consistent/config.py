from dataclasses import dataclass


@dataclass
class Config:
    partition_count: int
    replication_factor: int
    load: float
