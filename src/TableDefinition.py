class TableDefinition:
    seat = """
        CREATE TABLE IF NOT EXISTS seat (
            seat_id uuid,
            type text,
            PRIMARY KEY (seat_id)
        );
    """

    station = """
        CREATE TABLE IF NOT EXISTS station (
            station_id uuid,
            station_name text,
            PRIMARY KEY (station_id)
        );
    """

    reservation = """
        CREATE TABLE IF NOT EXISTS reservation (
            run_id uuid,
            seat_id uuid,
            PRIMARY KEY (run_id, seat_id)
        );
    """

    run = """
        CREATE TABLE IF NOT EXISTS run (
            run_id uuid,
            departure_station_id uuid,
            departure_time timestamp,
            arrival_station_id uuid,
            arrival_time timestamp,
            PRIMARY KEY (run_id)
        );
    """

    all = [seat, station, reservation, run]
