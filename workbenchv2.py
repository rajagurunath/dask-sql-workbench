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
        "Write SQL",
        "Explore Schema",
        "Create Python UDF",
        "Visualizer"
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
elif user_want_to == "Create Python UDF":
    create_pyUDF()
elif user_want_to == "Visualizer":
    visualizer_page()