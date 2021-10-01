import dask_sql
from pkg_resources import require
import streamlit as st
st.set_page_config(layout="wide")
from explore_schemas import explore_schema
from settings import settings
from sql_explorer import write_sql
from connections import connection_page


user_want_to= st.sidebar.radio(
    "Dask-SQL",
    options= [
        "Write SQL",
        "Explore Schema",
        "Visualizer",
        "Connections",
        "Settings"
    ]

)



if user_want_to == "Write SQL":
    write_sql()
elif user_want_to == "Settings":
    settings()
elif user_want_to == "Explore Schema":
    explore_schema()
elif user_want_to == "Connections":
    connection_page()
