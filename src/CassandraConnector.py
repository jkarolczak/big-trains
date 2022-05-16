from cassandra.cluster import Cluster

from Config import Config
from TableDefinition import TableDefinition


class CassandraConnector:
    def __init__(
            self,
            config: Config
    ) -> None:
        self._config = config
        self._cluster = Cluster([self._config.ip_address], self._config.port)
        self._session = None
        self._set_session()
        self._create_tables()

    def _set_session(self):
        self._session = self._cluster.connect()
        statement = f"CREATE KEYSPACE IF NOT EXISTS {self._config.keyspace} " + \
                    "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': " + \
                    f"'{self._config.replication_factor}'" + "}"
        self._session.execute(statement)
        self._session.set_keyspace(self._config.keyspace)

    def _create_tables(self):
        for table_definition in TableDefinition.all:
            self._session.execute(table_definition)
