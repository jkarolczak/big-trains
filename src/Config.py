import yaml
from cassandra import ConsistencyLevel

CONSISTENCY = {
    "one": ConsistencyLevel.ONE,
    "two": ConsistencyLevel.TWO,
    "three": ConsistencyLevel.THREE,
    "quorum": ConsistencyLevel.QUORUM,
    "all": ConsistencyLevel.ALL,
}


class Config:
    def __init__(
            self,
            path: str
    ) -> None:
        self._path = path
        self._config = self.read()

    def read(self):
        with open(self._path) as fp:
            config = yaml.safe_load(fp)
        return config

    @property
    def consistency(self):
        consistency = self._config["consistency"].lower()
        allowable_consistency = {"one", "two", "three", "quorum", "all"}
        if consistency.lower() not in allowable_consistency:
            raise ValueError(f"Consistency level should be one of {allowable_consistency} but found {consistency}")
        return CONSISTENCY[consistency]

    @property
    def ip_address(self) -> str:
        return self._config["ip_address"]

    @property
    def keyspace(self) -> str:
        return self._config["keyspace"]

    @property
    def port(self) -> int:
        return int(self._config["port"])

    @property
    def replication_factor(self) -> int:
        return int(self._config["replication_factor"])
