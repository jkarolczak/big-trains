from cassandra.cluster import Session, ResultSet


class QueryEngine:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.delete = self.Delete(self)
        self.drop = self.Drop(self)
        self.insert = self.Insert(self)
        self.select = self.Select(self)
        self.truncate = self.Truncate(self)

    def execute(self, query: str):
        return self.session.execute(query)

    class Delete:
        def __init__(self, engine: object) -> None:
            self._engine = engine
            self.seat = self.Seat(engine)
            self.station = self.Station(engine)
            self.reservation = self.Reservation(engine)
            self.run = self.Run(engine)

        class Seat:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_by_id = self._engine.session.prepare("DELETE FROM seat WHERE seat_id = ?")

            def by_id(self, seat_id: int) -> None:
                query = self._query_by_id.bind({'seat_id': seat_id})
                self._engine.execute(query)

        class Station:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_by_id = self._engine.session.prepare("DELETE FROM station WHERE station_id = ?")

            def by_id(self, station_id: int) -> None:
                query = self._query_by_id.bind({'seat_id': station_id})
                self._engine.execute(query)

        class Reservation:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_by_run_id = self._engine.session.prepare("DELETE FROM reservation WHERE run_id = ?")
                self._query_by_ids = self._engine.session.prepare(
                    "DELETE FROM reservation WHERE run_id = ? AND seat_id = ?")

            def by_run_id(self, run_id: int) -> None:
                query = self._query_by_run_id.bind({'run_id': run_id})
                self._engine.execute(query)

            def by_ids(self, run_id: int, seat_id: int) -> None:
                query = self._query_by_ids.bind({'run_id': run_id, 'seat_id': seat_id})
                self._engine.execute(query)

        class Run:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_by_id = self._engine.session.prepare("DELETE FROM run WHERE run_id = ?")

            def by_id(self, run_id: int) -> None:
                query = self._query_by_id.bind({'run_id': run_id})
                self._engine.execute(query)

    class Drop:
        def __init__(self, engine: object) -> None:
            self._engine = engine

        def _drop_table(self, name: str) -> None:
            query = f"DROP TABLE {name}"
            self._engine.execute(query)

        def seat(self) -> None:
            self._drop_table("seat")

        def station(self) -> None:
            self._drop_table("station")

        def reservation(self) -> None:
            self._drop_table("reservation")

        def run(self) -> None:
            self._drop_table("run")

    class Insert:
        def __init__(self, engine: object) -> None:
            self._engine = engine
            self._query_seat = self._engine.session.prepare(
                "INSERT INTO seat (seat_id, type) VALUES (blobAsUuid(timeuuidAsBlob(now())), ?)")
            self._query_station = self._engine.session.prepare(
                "INSERT INTO station (station_id, station_name) VALUES (blobAsUuid(timeuuidAsBlob(now())), ?)")
            self._query_reservation = self._engine.session.prepare(
                "INSERT INTO reservation (run_id, seat_id) VALUES (?, ?)")
            self._query_run = self._engine.session.prepare(
                "INSERT INTO run (run_id, departure_station_id, departure_time, arrival_station_id, "
                "arrival_time) VALUES (blobAsUuid(timeuuidAsBlob(now())), ?, ?, ?, ?)")

        def seat(self, type_: str):
            query = self._query_seat.bind({'type': type_})
            self._engine.execute(query)

        def station(self, station_name: str):
            query = self._query_station.bind({'station_name': station_name})
            self._engine.execute(query)

        def reservation(self, run_id: int, seat_id: int):
            query = self._query_reservation.bind({'run_id': run_id, 'seat_id': seat_id})
            self._engine.execute(query)

        def run(self, departure_station_id: int, departure_time: int, arrival_station_id: int,
                arrival_time: int):
            query = self._query_run.bind({'departure_station_id': departure_station_id,
                                          'departure_time': departure_time, 'arrival_station_id': arrival_station_id,
                                          'arrival_time': arrival_time})
            self._engine.execute(query)

    class Select:
        def __init__(self, engine: object) -> None:
            self._engine = engine
            self.seat = self.Seat(engine)
            self.station = self.Station(engine)
            self.reservation = self.Reservation(engine)
            self.run = self.Run(engine)

        class Seat:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_all = "SELECT * FROM seat"
                self._query_by_id = self._engine.session.prepare("SELECT * FROM seat WHERE seat_id = ?")

            def all(self):
                query = self._query_all
                return self._engine.execute(query)

            def by_id(self, seat_id: int) -> ResultSet:
                query = self._query_by_id.bind({'seat_id': seat_id})
                return self._engine.execute(query).one()

        class Station:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_all = "SELECT * FROM station"
                self._query_by_id = self._engine.session.prepare("SELECT * FROM station WHERE station_id = ?")
                self._query_by_name = self._engine.session.prepare(
                    "SELECT * FROM station WHERE station_name = ? ALLOW FILTERING")

            def all(self) -> ResultSet:
                query = self._query_all
                return self._engine.execute(query)

            def by_id(self, station_id: int) -> ResultSet:
                query = self._query_by_id.bind({'station_id': station_id})
                return self._engine.execute(query).one()

            def by_name(self, station_name: str) -> ResultSet:
                query = self._query_by_id.bind({'station_name': station_name})
                return self._engine.execute(query)

        class Reservation:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_all = "SELECT * FROM reservation"
                self._query_by_run_id = self._engine.session.prepare("SELECT * FROM reservation WHERE run_id = ?")

            def all(self) -> ResultSet:
                query = self._query_all
                return self._engine.execute(query)

            def by_run_id(self, run_id: int) -> ResultSet:
                query = self._query_by_id.bind({'run_id': run_id})
                return self._engine.execute(query)

        class Run:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_all = "SELECT * FROM run"
                self._query_by_id = self._engine.session.prepare("SELECT * FROM run WHERE run_id = ?")
                self._query_by_arrival_id = self._engine.session.prepare(
                    "SELECT * FROM run WHERE arrival_station_id = ? ALLOW FILTERING")
                self._query_by_departure_id = self._engine.session.prepare(
                    "SELECT * FROM run WHERE departure_station_id = ? ALLOW FILTERING")

            def all(self) -> ResultSet:
                query = self._query_all
                return self._engine.execute(query)

            def by_id(self, run_id: int) -> ResultSet:
                query = self._query_by_id.bind({'run_id': run_id})
                return self._engine.execute(query).one()

            def by_arrival_station_id(self, arrival_station_id: int) -> ResultSet:
                query = self._query_by_arrival_id.bind({'arrival_station_id': arrival_station_id})
                return self._engine.execute(query)

            def by_departure_station_id(self, departure_station_id: int) -> ResultSet:
                query = self._query_by_arrival_id.bind({'departure_station_id': departure_station_id})
                return self._engine.execute(query)

    class Truncate:
        def __init__(self, engine: object) -> None:
            self._engine = engine

        def _truncate_table(self, name: str) -> None:
            query = f"TRUNCATE {name}"
            self._engine.execute(query)

        def seat(self) -> None:
            self._truncate_table("seat")

        def station(self) -> None:
            self._truncate_table("station")

        def reservation(self) -> None:
            self._truncate_table("reservation")

        def run(self) -> None:
            self._truncate_table("run")
