class TableDefinition:
    run_by_id = """
        CREATE TABLE IF NOT EXISTS run_by_id (
            run_id uuid,
            departure_station text,
            departure_date date,
            departure_time time,
            arrival_station text,
            travel_time int,
            carrier text,
            PRIMARY KEY (run_id)
        );
    """

    run_by_departure = """
        CREATE TABLE IF NOT EXISTS run_by_departure (
            departure_station text,
            departure_date date,
            departure_time time,
            arrival_station text,
            run_id uuid,
            PRIMARY KEY ((departure_station, departure_date), arrival_station, departure_time)
        );
    """

    seat = """
        CREATE TABLE IF NOT EXISTS seat (
            run_id uuid,
            seat_no int,
            is_available boolean,
            seat_type text,
            PRIMARY KEY ((run_id), seat_no)
        );
    """

    reservation_by_run = """
        CREATE TABLE IF NOT EXISTS reservation_by_run (
            run_id uuid,
            seat_no int,
            client_email text,
            reservation_timestamp timestamp,
            PRIMARY KEY ((run_id), seat_no)
        );
    """

    reservation_by_client = """
        CREATE TABLE IF NOT EXISTS reservation_by_client (
            client_email text,
            run_id uuid,
            seat_no int,
            PRIMARY KEY ((client_email), run_id, seat_no)
        )
    """

    all = [run_by_id, run_by_departure, seat, reservation_by_run, reservation_by_client]


