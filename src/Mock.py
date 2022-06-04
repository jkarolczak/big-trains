import datetime
import time

import numpy as np


class Mock:
    @property
    def date(self) -> datetime.date:
        now = datetime.date.today()
        delta = datetime.timedelta(days=np.random.randint(1, 31))
        return now + delta

    @property
    def time(self) -> time:
        hour = np.random.randint(0, 24)
        minutes = np.random.randint(0, 60)
        return datetime.datetime.strptime(f'{hour}:{minutes}', '%H:%M').time()

    @property
    def now(self):
        return datetime.datetime.now()

    @property
    def travel_time(self) -> int:
        return np.random.randint(90, 360)

    @property
    def email(self) -> str:
        name = np.random.choice(["person", "personage", "human", "man", "woman", "being", "body", "individual"], 1)[0]
        domain = np.random.choice(
            ["gmail.com", "outlook.com", "yahoo.com", "hotmail.com", "mail.com", "wp.pl", "o2.pl", "buziaczek.pl"], 1)[
            0]
        return f'{name}@{domain}'

    @property
    def seat(self):
        return self.Seat()

    @property
    def station(self) -> str:
        return np.random.choice(
            ["Szczecin", "Gdańsk", "Gdynia", "Olsztyn", "Białystok", "Warszawa", "Poznań", "Łódź", "Wrocław",
             "Częstochowa", "Lublin", "Kielce", "Kraków", "Rzeszów"], 1)[0]

    class Seat:
        def __init__(self):
            self.compartment = np.random.randint(1, 12)
            self._no = np.random.randint(1, 7)
            self._type = self.no_to_type(self._no)

        def no_to_type(self, seat_no: int):
            if seat_no <= 2:
                return "aisle"
            if seat_no >= 5:
                return "window"
            return "middle"

        @property
        def no(self) -> int:
            return int(str(self.compartment) + str(self._no))

        @property
        def type(self) -> str:
            return self._type


if __name__ == "__main__":
    mock = Mock()
    seat = mock.seat
    station = mock.station
    print(seat.no, seat.type)
    print(mock.station)
    print(mock.email)
    print(mock.date)
    print(mock.now)  # reservation_timestamp
    print(mock.travel_time)
