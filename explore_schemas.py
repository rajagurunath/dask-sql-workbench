
from connections import get_dask_sql_context
import streamlit as st
from defaults import SHOW_TABLE_ROWS
import pandas as pd

dask_sql_context = get_dask_sql_context()

def explore_schema():
    schemas = dask_sql_context.schema.keys()

    selected_schema = st.selectbox(
        "Schemas",
        options=schemas,

    )

    tables = dask_sql_context.schema[selected_schema].tables
    experiments = dask_sql_context.schema[selected_schema].experiments
    models = dask_sql_context.schema[selected_schema].models
    funcs = dask_sql_context.schema[selected_schema].functions

    col1, col2,col3,col4 = st.columns(4)

    placeholder = st.empty()
    # st.session_state["on_changed_called_on"] = None

    def tables_changed():
        st.session_state['on_changed_called_on'] = "tables"

    def functions_changed():
        st.session_state['on_changed_called_on'] = "func"
    
    def models_changed():
        st.session_state['on_changed_called_on'] = "models"

    def experiments_changed():
        st.session_state['on_changed_called_on'] = "experiments"

    with col1:
        st.subheader("Tables")
        selected_table = st.selectbox("Tables",options=tables,
        on_change = tables_changed,
        key="selected_table"
        )

    with col2:
        st.subheader("Functions")
        selected_func = st.selectbox("Functions",options=funcs,
        on_change=functions_changed,
        key="selected_func"
        )

    with col3:
        st.subheader("Models")
        selected_model = st.selectbox("Models",options=models,
        on_change=models_changed,
        key="selected_model"
        )

    with col4:
        st.subheader("Experiments")
        selected_experiment = st.selectbox("Experiments",options=experiments,
        on_change=experiments_changed,
        key="selected_experiment"
        )

    if "on_changed_called_on" in st.session_state:
        if st.session_state["on_changed_called_on"] =="tables":
            st.write(st.session_state['selected_table'])
            st.table(dask_sql_context.schema[selected_schema].tables[st.session_state['selected_table']].df.head(SHOW_TABLE_ROWS))
        
        elif st.session_state["on_changed_called_on"] == "func":
            selected_func = st.session_state['selected_func']
            st.write(selected_func)
            st.table(pd.DataFrame(dask_sql_context.schema[selected_schema].function_lists).astype(str))
            st.code(dask_sql_context.schema[selected_schema].functions[selected_func])
        
        elif st.session_state["on_changed_called_on"] == "models":
            model = st.session_state['selected_model']
            st.write(f"Describe {model}")
            # st.write(dask_sql_context.schema[selected_schema].models[st.session_state['selected_model']])
            st.table(dask_sql_context.sql(f"DESCRIBE MODEL {model}").compute().astype(str))
        
        elif st.session_state["on_changed_called_on"] == "experiments":
            selected_experiment = st.session_state['selected_experiment']
            st.write(f"Describe Experiment {selected_experiment}")
            st.table(dask_sql_context.schema[selected_schema].experiments[selected_experiment].astype(str))

        
        
    
    
    