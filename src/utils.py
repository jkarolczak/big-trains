from Mock import Mock
from src import CassandraConnector


def fill_database(client: CassandraConnector, runs: int = 20) -> None:
    mock = Mock()
    for _ in range(runs):
        station1, station2 = mock.station, mock.station
        while station1 == station2:
            station2 = mock.station
        run_id = client.query_engine.insert.run(station1, mock.date, mock.time, station2, mock.travel_time,
                                                mock.carrier)
        for seat in mock.carriage:
            client.query_engine.insert.seat(run_id, seat.no, seat.type)
