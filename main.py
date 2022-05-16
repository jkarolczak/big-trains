from src import Config, CassandraConnector


def main() -> None:
    config = Config("config.yaml")
    client = CassandraConnector(config)
    # print(client._config.consistency)


if __name__ == "__main__":
    main()
