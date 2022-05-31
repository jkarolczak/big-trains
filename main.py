from datetime import datetime  # noqa

from src import Config, CassandraConnector


def main() -> None:
    config = Config("config.yaml")
    client = CassandraConnector(config)
    client.query_engine.truncate.seat()
    client.query_engine.insert.seat("window")
    client.query_engine.insert.seat("aisle")
    client.query_engine.insert.station("Poznań")
    client.query_engine.insert.station("Warszawa")
    # departure = datetime.strptime('01/01/2023 10:20:00', '%d/%m/%Y %H:%M:%S')
    # arrival = datetime.strptime('01/01/2023 13:40:00', '%d/%m/%Y %H:%M:%S')
    # client.query_engine.insert.run(1, departure, 2, arrival)
    # client.query_engine.insert.reservation(1, 1)

    # print(client.query_engine.select.run.by_id(1))


if __name__ == "__main__":
    main()
