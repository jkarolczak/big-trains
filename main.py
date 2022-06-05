import uuid
from datetime import datetime, date, time  # noqa

from src import Config, CassandraConnector, QueryEngine


def main() -> None:
    config = Config("config.yaml")
    client = CassandraConnector(config)

    client.query_engine.truncate.all()

    run_date = date(2022, 6, 2)
    run_time = time(8, 15, 0)

    new_uuid_1 = client.query_engine.insert.run("Konin", run_date, run_time, "Poznan", 45, "PKP Intercity")
    new_uuid_2 = client.query_engine.insert.run("Poznan", run_date, run_time, "Warszawa", 45, "KW")
    new_uuid_3 = client.query_engine.insert.run("Konin", date(2022, 6, 3), run_time, "Warszawa", 45, "KW")

    client.query_engine.insert.seat(new_uuid_1, 1, 'window')
    client.query_engine.insert.reservation(new_uuid_1, 1, 'konszewczyk@outlook.com', datetime.now())

    print(client.query_engine.select.seat.all().all())
    print(client.query_engine.select.reservation.all().all())

    client.query_engine.delete.reservation.by_seat(new_uuid_1, 1)
    print(client.query_engine.select.seat.all().all())


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
