from src.pages import *


def main() -> None:
    client = None
    try:
        config = Config("config.yaml")
        client = CassandraConnector(config)
    except:
        st.error("Application cannot connect to the database. Try again later.")

    if client is not None:
        st.set_page_config(
            page_title="BIG TRAINS",
            page_icon="🚆",
            layout="wide",
            initial_sidebar_state="expanded",
        )
        if 'config' not in st.session_state:
            st.session_state["config"] = config
        if 'client' not in st.session_state:
            st.session_state["client"] = client
        st.sidebar.title(f"🚂 BIG TRAINS")
        pages = {
            "Admin": admin_page,
            "Client": client_page,
            "Stress tests": stress_tests
        }
        name = st.sidebar.radio('Select step', pages.keys(), index=0)
        pages[name]()


if __name__ == "__main__":
    main()
