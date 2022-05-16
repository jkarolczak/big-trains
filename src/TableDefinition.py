class TableDefinition:
    seat = """
        CREATE TABLE IF NOT EXISTS seat (
            seat_id int,
            type text,
            PRIMARY KEY (seat_id)
        );
    """

    station = """
        CREATE TABLE IF NOT EXISTS station (
            station_id int,
            station_name text,
            PRIMARY KEY (station_id)
        );
    """

    reservation = """
        CREATE TABLE IF NOT EXISTS reservation (
            run_id int,
            seat_id int,
            PRIMARY KEY (run_id, seat_id)
        );
    """

    run = """
        CREATE TABLE IF NOT EXISTS run (
            run_id int,
            departure_station_id int,
            departure_time timestamp,
            arrival_station_id int,
            arrival_time timestamp,
            PRIMARY KEY (run_id)
        );
    """

    all = [seat, station, reservation, run]
