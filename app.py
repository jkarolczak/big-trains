import pandas as pd
import streamlit as st

from src import CassandraConnector, Config, utils


def admin() -> None:
    st.title("Admin panel")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Fill database")
        if st.button("Create 20 artificial runs"):
            utils.fill_database(st.session_state["client"], 20)
    with col2:
        st.subheader("Truncate all tables")
        if st.button("Truncate all tables"):
            st.session_state["client"].query_engine.truncate.all()


def reservations() -> None:
    st.title("Reservations")


def timetable() -> None:
    st.title("Timetable")
    table = {"departure station": [], "departure date": [], "departure time": [], "arrival station": []}
    for row in st.session_state["client"].query_engine.select.run.all():
        table["departure date"].append(row.departure_date.date())
        table["departure station"].append(row.departure_station)
        table["departure time"].append(row.departure_time.time().strftime("%H:%M"))
        table["arrival station"].append(row.arrival_station)
    st.dataframe(pd.DataFrame(table))


def main() -> None:
    # backend
    config = Config("config.yaml")
    client = CassandraConnector(config)

    if 'client' not in st.session_state:
        st.session_state["client"] = client

    # ui
    st.set_page_config(
        page_title="BIG TRAINS",
        page_icon="🚆",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.sidebar.title(f"🚂 BIG TRAINS")
    pages = {
        "Admin panel": admin,
        "Timetable": timetable,
        "Reservations": reservations
    }
    name = st.sidebar.radio('Select step', pages.keys(), index=0)
    pages[name]()


if __name__ == "__main__":
    main()
