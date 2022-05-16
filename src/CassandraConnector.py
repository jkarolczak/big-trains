from cassandra.cluster import Cluster

import Config


class CassandraConnector:
    def __init__(
            self,
            config: Config
    ) -> None:
        self._config = config
        self.cluster = Cluster([self._config.ip_address], self._config.port)
        self.session = None
        self.set_session()

    def set_session(self):
        self.session = self.cluster.connect()
        statement = f"CREATE KEYSPACE IF NOT EXISTS {self._config.keyspace} " + \
                    "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': " + \
                    f"'{self._config.replication_factor}'" + "}"
        self.session.execute(statement)
        self.session.set_keyspace(self._config.keyspace)
