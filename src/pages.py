import datetime
import re
import time
import uuid
from multiprocessing import Pool
from typing import List, Tuple

import numpy as np
import pandas as pd
import streamlit as st

import utils
from Mock import Mock
from src import CassandraConnector, Config


def admin_page() -> None:
    st.title("Admin panel")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Fill database")
        with st.form("Add runs"):
            runs_no = st.number_input("Number of runs to insert", min_value=1, max_value=100, value=1, step=1)
            if st.form_submit_button("Add runs"):
                try:
                    utils.fill_database(st.session_state["client"], runs_no)
                    st.info(f"{runs_no} new run(s) has(ve) been added to the database.")
                except:
                    st.error(f"Cannot fill the database with {runs_no} new runs. Try again later")
    with col2:
        st.subheader("Truncate all tables")
        if st.button("Truncate all tables"):
            try:
                st.session_state["client"].query_engine.truncate.all()
            except:
                st.error(f"Cannot truncate tables. Try again later")


def client_page() -> None:
    timetable()
    details()


def details() -> None:
    st.title("Run details")
    run_details = None
    with st.form("run_details"):
        run_id = st.text_input("Run identifier")
        if st.form_submit_button():
            try:
                run_details = st.session_state["client"].query_engine.select.run.by_id(uuid.UUID(run_id))
            except:
                st.write("Request couldn't been processed. Verify if the id identifier is correct and try again.")

    if run_details is not None:
        rd, rt = run_details.departure_date.date(), run_details.departure_time.time()
        rddt = datetime.datetime(rd.year, rd.month, rd.day, rt.hour, rt.minute)
        radt = rddt + datetime.timedelta(minutes=run_details.travel_time)
        st.markdown(f"**Departure**: {run_details.departure_station}, {rddt}")
        st.markdown(f"**Arrival**: {run_details.arrival_station}, {radt}")
        st.markdown(f"**Carrier**: {run_details.carrier}")

        reservations_ = None
        try:
            reservations_ = st.session_state["client"].query_engine.select.reservation.by_run(uuid.UUID(run_id))
        except:
            st.write("Request couldn't been processed. Verify if the id identifier is correct and try again.")
        if reservations_ is not None:
            table = {"no": [], "client": []}
            for reservation in reservations_:
                table["no"].append(reservation.seat_no)
                table["client"].append(reservation.client_email)


def timetable() -> None:
    st.title("Timetable")
    table = {"departure station": [], "departure date": [], "departure time": [], "arrival station": [],
             "identifier": []}
    runs = None
    try:
        runs = st.session_state["client"].query_engine.select.run.all()
    except:
        st.write("Timetable couldn't been load. Verify your connection and try again.")

    if runs is not None:
        for run in runs:
            table["departure date"].append(run.departure_date.date())
            table["departure station"].append(run.departure_station)
            table["departure time"].append(run.departure_time.time().strftime("%H:%M"))
            table["arrival station"].append(run.arrival_station)
            table["identifier"].append(str(run.run_id))
        st.dataframe(pd.DataFrame(table))


def reservation() -> None:
    st.title("Reservation")
    run_id = st.text_input("Run identifier")
    if st.button("Select run"):
        seats_raw = st.session_state["client"].query_engine.select.seat.by_run(uuid.UUID(run_id)).all()
        seats = [f"{seat.seat_no} ({seat.seat_type})" for seat in seats_raw]
        seat = st.selectbox("Seat no.", options=seats)
        if st.button("Select seat"):
            seat_no = int(re.search(r'\d+', seat).group())
            try:
                st.session_state["client"].query_engine.insert.reservation(uuid.UUID(run_id), seat_no)
            except:
                st.write("Request couldn't been processed. Verify if the id identifier is correct and try again.")


def stress_tests() -> None:
    stress_test_1()
    stress_test_2()
    stress_test_3()


def stress_test_1() -> None:
    st.title("Stress test 1")
    if st.button("run", key="run_stress_test_1"):
        run_id = None
        try:
            run_id = st.session_state["client"].query_engine.select.run.all()[0].run_id
        except:
            st.markdown("Create at least one run before running stress test.")

        if run_id is not None:
            seat_no = None
            try:
                seat_no = st.session_state["client"].query_engine.select.seat.by_run(run_id).all()[0].seat_no
            except:
                st.markdown(f"Create at least one seat for run {run_id} before running stress test.")
            if seat_no is not None:
                mock = Mock()
                logs = []
                for i in range(1000):
                    try:
                        st.session_state["client"].query_engine.insert.reservation(run_id, seat_no, mock.email,
                                                                                   mock.now)
                        logs.append(f"{i}. Success")
                    except:
                        logs.append(f"{i}. Failure")
                with st.expander("logs"):
                    st.markdown("\n".join(logs))
                with st.expander("raw query output"):
                    st.markdown(
                        st.session_state["client"].query_engine.select.reservation.by_seat(run_id, seat_no))


def stress_test_2() -> None:
    pass


def stress_test_3_agent(args: Tuple[str, uuid.UUID, List[int], Config]) -> None:
    # client = CassandraConnector(st.session_state["config"])
    # client = st.session_state["client"]
    mock = Mock()
    email, run_id, seat_nos, config = args
    client = CassandraConnector(config)
    np.random.shuffle(seat_nos)
    for seat_no in seat_nos:
        time.sleep(0.0001)
        try:
            client.query_engine.insert.reservation(run_id, seat_no, email, mock.now)
            print(email, seat_no)
        except:
            continue
    return 0


def stress_test_3() -> None:
    st.title("Stress test 3")
    if st.button("run", key="run_stress_test_3"):
        run_id = None
        try:
            run_id = st.session_state["client"].query_engine.select.run.all()[0].run_id
        except:
            st.markdown("Create at least one run before running stress test.")

        if run_id is not None:
            seat_nos = None
            try:
                seat_nos = [seat.seat_no for seat in
                            st.session_state["client"].query_engine.select.seat.by_run(run_id).all()]
            except:
                st.markdown(f"Create at least one seat for run {run_id} before running stress test.")
            if seat_nos is not None:
                size = 2
                mock = Mock()
                run_id_ = [run_id for _ in range(size)]
                seat_nos_ = [seat_nos for _ in range(size)]
                email_ = [mock.email for _ in range(size)]
                config_ = [st.session_state["config"] for _ in range(size)]
                with st.spinner("Simulating load"):
                    with Pool(size) as p:
                        p.map(stress_test_3_agent, zip(email_, run_id_, seat_nos_, config_))

                with st.expander("raw query output"):
                    st.markdown(
                        st.session_state["client"].query_engine.select.reservation.by_run(run_id).all())
