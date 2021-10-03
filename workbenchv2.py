import dask_sql
from pkg_resources import require
import streamlit as st
st.set_page_config(layout="wide")
from explore_schemas import explore_schema
from settings import settings
from sql_explorer import write_sql
from connections import connection_page
from udfs import create_pyUDF
from visualizer import visualizer_page

user_want_to= st.sidebar.radio(
    "Dask-SQL",
    options= [
        "Connections",
        "Settings",
        "Dask-SQL Workbench",
        "Explore 🕵️ ",
        "Python's Nest 🐍🥚",
        "Visualizer"
    ]

)


if user_want_to == "Dask-SQL Workbench":
    write_sql()
elif user_want_to == "Settings":
    settings()
elif user_want_to == "Explore 🕵️ ":
    explore_schema()
elif user_want_to == "Connections":
    connection_page()
elif user_want_to == "Python's Nest 🐍🥚":
    create_pyUDF()
elif user_want_to == "Visualizer":
    visualizer_page()