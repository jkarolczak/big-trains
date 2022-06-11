from cassandra.cluster import Session, ResultSet
import uuid
import datetime


class QueryEngine:
    class DuplicateException(Exception):
        pass

    def __init__(self, session: Session) -> None:
        self.session = session
        self.select = self.Select(self)
        self.delete = self.Delete(self)
        self.drop = self.Drop(self)
        self.insert = self.Insert(self)
        self.truncate = self.Truncate(self)

    def execute(self, query: str):
        return self.session.execute(query)

    class Delete:
        def __init__(self, engine: object) -> None:
            self._engine = engine
            self.reservation = self.Reservation(engine)

        class Reservation:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_by_seat = self._engine.session.prepare(
                    "DELETE FROM reservation_by_run WHERE run_id = ? AND seat_no = ?")
                self._query_by_client = self._engine.session.prepare(
                    "DELETE FROM reservation_by_client WHERE client_email = ? AND run_id = ? AND seat_no = ?"
                )
                self._query_update_seat = self._engine.session.prepare(
                    "UPDATE seat SET is_available = true WHERE run_id = ? AND seat_no = ?"
                )

            def by_seat(self, run_id: uuid.UUID, seat_no: int):
                result_set = self._engine.select.reservation.by_seat(run_id, seat_no)
                if result_set is None:
                    return
                client_email = result_set.client_email

                query = self._query_by_seat.bind((run_id, seat_no))
                self._engine.execute(query)

                query = self._query_by_client.bind((client_email, run_id, seat_no))
                self._engine.execute(query)

                query = self._query_update_seat.bind((run_id, seat_no))
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
            self._query_seat_update = self._engine.session.prepare(
                "UPDATE seat SET is_available = false WHERE run_id = ? AND seat_no = ?"
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

            query = self._query_seat_update.bind(
                (run_id, seat_no)
            )
            self._engine.execute(query)

    class Select:
        def __init__(self, engine: object) -> None:
            self._engine = engine
            self.run = self.Run(engine)
            self.seat = self.Seat(engine)
            self.reservation = self.Reservation(engine)

        class Run:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_all = "SELECT * FROM run_by_id"
                self._query_by_id = self._engine.session.prepare("SELECT * FROM run_by_id WHERE run_id = ?")
                self._query_by_departure = self._engine.session.prepare(
                    "SELECT * FROM run_by_departure WHERE departure_station = ? AND departure_date = ?")
                self._query_by_departure_and_arrival = self._engine.session.prepare(
                    "SELECT * FROM run_by_departure WHERE departure_station = ? AND departure_date = ? "
                    "AND arrival_station = ?")
                self._query_by_full_departure = self._engine.session.prepare(
                    "SELECT * FROM run_by_departure WHERE departure_station = ? AND departure_date = ? "
                    "AND arrival_station = ? AND departure_time >= ? AND departure_time <= ?")

            def all(self) -> ResultSet:
                query = self._query_all
                return self._engine.execute(query)

            def by_id(self, run_id: uuid.UUID) -> ResultSet:
                query = self._query_by_id.bind((run_id,))
                return self._engine.execute(query).one()

            def by_departure(self, departure_station: str, departure_date: datetime.date, arrival_station: str = None,
                             departure_time: tuple = (None, None)) -> ResultSet:
                if arrival_station is None:
                    query = self._query_by_departure.bind((departure_station, departure_date))
                elif departure_time[0] is None and departure_time[1] is None:
                    query = self._query_by_departure_and_arrival.bind(
                        (departure_station, departure_date, arrival_station))
                else:
                    time_lb = departure_time[0] if departure_time[0] is not None else datetime.time(0, 0, 0)
                    time_ub = departure_time[1] if departure_time[1] is not None else datetime.time(24, 0, 0)
                    query = self._query_by_full_departure.bind((departure_station, departure_date,
                                                                arrival_station, time_lb, time_ub))

                return self._engine.execute(query)


        class Seat:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_all = "SELECT * FROM seat"
                self._query_by_run = self._engine.session.prepare("SELECT * FROM seat WHERE run_id = ?")
                self._query_by_seat = self._engine.session.prepare("SELECT * FROM seat WHERE run_id = ? AND seat_no = ?")

            def all(self):
                query = self._query_all
                return self._engine.execute(query)

            def by_run(self, run_id: uuid.UUID) -> ResultSet:
                query = self._query_by_run.bind((run_id,))
                return self._engine.execute(query)

            def by_seat(self, run_id: uuid.UUID, seat_no: int) -> ResultSet:
                query = self._query_by_seat.bind((run_id, seat_no))
                return self._engine.execute(query).one()

        class Reservation:
            def __init__(self, engine: object) -> None:
                self._engine = engine
                self._query_all = self._engine.session.prepare("SELECT * FROM reservation_by_run")
                self._query_by_run = self._engine.session.prepare("SELECT * FROM reservation_by_run WHERE run_id = ?")
                self._query_by_seat = self._engine.session.prepare(
                    "SELECT * FROM reservation_by_run WHERE run_id = ? and seat_no = ?")
                self._query_by_client = self._engine.session.prepare(
                    "SELECT * FROM reservation_by_client WHERE client_email = ?")
                self._query_by_client_and_run = self._engine.session.prepare(
                    "SELECT * FROM reservation_by_client WHERE client_email = ? AND run_id = ?")
                self._query_by_client_and_seat = self._engine.session.prepare(
                    "SELECT * FROM reservation_by_client WHERE client_email = ? AND run_id = ? AND seat_no = ?")

            def all(self):
                query = self._query_all
                return self._engine.execute(query)

            def by_run(self, run_id: uuid.UUID):
                query = self._query_by_run.bind((run_id,))
                return self._engine.execute(query)

            def by_seat(self, run_id: uuid.UUID, seat_no: int) -> ResultSet:
                query = self._query_by_seat.bind((run_id, seat_no))
                return self._engine.execute(query).one()

            def by_client(self, client_email: str) -> ResultSet:
                query = self._query_by_client.bind((client_email,))
                return self._engine.execute(query)

            def by_client_and_run(self, client_email: str, run_id: uuid.UUID) -> ResultSet:
                query = self._query_by_client_and_run.bind((client_email, run_id))
                return self._engine.execute(query)

            def by_client_and_seat(self, client_email: str, run_id: uuid.UUID, seat_no: int) -> ResultSet:
                query = self._query_by_client_and_seat.bind((client_email, run_id, seat_no))
                return self._engine.execute(query).one()


    class Truncate:
        def __init__(self, engine: object) -> None:
            self._engine = engine

        def _truncate_table(self, name: str) -> None:
            query = f"TRUNCATE {name}"
            self._engine.execute(query)

        def run_by_id(self) -> None:
            self._truncate_table("run_by_id")

        def run_by_departure(self) -> None:
            self._truncate_table("run_by_departure")

        def seat(self) -> None:
            self._truncate_table("seat")

        def reservation_by_run(self) -> None:
            self._truncate_table("reservation_by_run")

        def reservation_by_client(self) -> None:
            self._truncate_table("reservation_by_client");

        def all(self) -> None:
            self.run_by_id()
            self.run_by_departure()
            self.seat()
            self.reservation_by_run()
            self.reservation_by_client()
