import uuid
from datetime import datetime, date, time  # noqa

from src import Config, CassandraConnector, QueryEngine


def main() -> None:
    config = Config("config.yaml")
    client = CassandraConnector(config)

    run_date = date(2022, 6, 2)
    run_time = time(8, 15, 0)
    new_uuid = client.query_engine.insert.run("Test", run_date, run_time, "Test", 45, "PKP Intercity")
    print(new_uuid)

    try:
        client.query_engine.insert.run("Poznan", run_date, run_time, "Test", 45, "PKP Intercity")
    except QueryEngine.DuplicateException:
        print("duplicate row")

    client.query_engine.insert.seat(new_uuid, 1, "window")
    client.query_engine.insert.reservation(new_uuid, 1, "human@outlook.com", datetime.now())

    #client.query_engine.truncate.seat()
    #client.query_engine.insert.seat("window")
    #client.query_engine.insert.seat("aisle")
    #client.query_engine.insert.station("Poznań")
    #client.query_engine.insert.station("Warszawa")
    # departure = datetime.strptime('01/01/2023 10:20:00', '%d/%m/%Y %H:%M:%S')
    # arrival = datetime.strptime('01/01/2023 13:40:00', '%d/%m/%Y %H:%M:%S')
    # client.query_engine.insert.run(1, departure, 2, arrival)
    # client.query_engine.insert.reservation(1, 1)

    # print(client.query_engine.select.run.by_id(1))


if __name__ == "__main__":
    main()
