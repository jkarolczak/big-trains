from cassandra.cluster import Session, ResultSet
import uuid
import datetime


class QueryEngine:

    class DuplicateException(Exception):
        pass

    def __init__(self, session: Session) -> None:
        self.session = session
        #self.delete = self.Delete(self)
        self.drop = self.Drop(self)
        self.insert = self.Insert(self)
        #self.select = self.Select(self)
        #self.truncate = self.Truncate(self)

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

        def run_by_id(self) -> None:
            self._drop_table("run_by_id")

        def run_by_departure(self) -> None:
            self._drop_table("run_by_departure")

        def seat(self) -> None:
            self._drop_table("seat")

        def reservation_by_run(self) -> None:
            self._drop_table("reservation_by_run")

        def reservation_by_client(self) -> None:
            self._drop_table("reservation_by_client");

        def all(self) -> None:
            self.run_by_id()
            self.run_by_departure()
            self.seat()
            self.reservation_by_run()
            self.reservation_by_client()

    class Insert:
        def __init__(self, engine: object) -> None:
            self._engine = engine

            self._query_seat = self._engine.session.prepare(
                "INSERT INTO seat (run_id, seat_no, is_available, seat_type) VALUES (?, ?, ?, ?) IF NOT EXISTS"
            )
            self._query_run_by_id = self._engine.session.prepare(
                "INSERT INTO run_by_id (run_id, departure_station, departure_date, departure_time, arrival_station, "
                "travel_time, carrier) VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
            )
            self._query_run_by_departure = self._engine.session.prepare(
                "INSERT INTO run_by_departure (departure_station, departure_date, arrival_station, departure_time, "
                "run_id) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS"
            )
            self._query_reservation_by_run = self._engine.session.prepare(
                "INSERT INTO reservation_by_run (run_id, seat_no, client_email, reservation_timestamp) "
                "VALUES (?, ?, ?, ?)  IF NOT EXISTS"
            )
            self._query_reservation_by_client = self._engine.session.prepare(
                "INSERT INTO reservation_by_client (client_email, run_id, seat_no) VALUES (?, ?, ?) IF NOT EXISTS"
            )

        def seat(self, run_id: uuid.UUID, seat_no: int, seat_type: str, is_available: bool = True):
            query = self._query_seat.bind((run_id, seat_no, is_available, seat_type))
            query_rs = self._engine.execute(query)
            success = query_rs.one().applied
            if not success:
                raise QueryEngine.DuplicateException()

        def run(self, departure_station: str, departure_date: datetime.date, departure_time: datetime.time,
                arrival_station: str, travel_time: int, carrier: str) -> uuid.UUID:
            run_uuid = uuid.uuid1()

            query = self._query_run_by_departure.bind(
                (departure_station, departure_date, arrival_station, departure_time, run_uuid)
            )
            query_rs = self._engine.execute(query)
            success = query_rs.one().applied
            if not success:
                raise QueryEngine.DuplicateException()

            query = self._query_run_by_id.bind(
                (run_uuid, departure_station, departure_date, departure_time, arrival_station, travel_time, carrier)
            )
            self._engine.execute(query)
            return run_uuid

        def reservation(self, run_id: uuid.UUID, seat_no: int, client_email: str,
                        reservation_timestamp: datetime.datetime):
            query = self._query_reservation_by_run.bind(
                (run_id, seat_no, client_email, reservation_timestamp)
            )
            query_rs = self._engine.execute(query)
            success = query_rs.one().applied
            if not success:
                raise QueryEngine.DuplicateException()

            query = self._query_reservation_by_client.bind(
                (client_email, run_id, seat_no)
            )
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
                self._query_all = "SELECT * FROM seat_by_"
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
