import inspect

import pandas as pd
import streamlit as st
from connections import get_dask_sql_context
from defaults import SHOW_TABLE_ROWS

dask_sql_context = get_dask_sql_context()


def explore_schema():

    st.markdown(
        """
        ### Explore whole anatomy of the dask-sql

        1. Tables/views
        2. Models
        3. Experiments
        4. Custom Created 🐍 Functions
        """
    )

    schemas = dask_sql_context.schema.keys()

    selected_schema = st.selectbox("Schemas", options=schemas,)

    tables = list(dask_sql_context.schema[selected_schema].tables)
    experiments = list(dask_sql_context.schema[selected_schema].experiments)
    models = list(dask_sql_context.schema[selected_schema].models)
    funcs = list(dask_sql_context.schema[selected_schema].functions)

    tables.insert(0, None)
    funcs.insert(0, None)
    experiments.insert(0, None)
    models.insert(0, None)
    col1, col2, col3, col4 = st.columns(4)

    placeholder = st.empty()
    # st.session_state["on_changed_called_on"] = None

    def tables_changed():
        st.session_state["on_changed_called_on"] = "tables"

    def functions_changed():
        st.session_state["on_changed_called_on"] = "func"

    def models_changed():
        st.session_state["on_changed_called_on"] = "models"

    def experiments_changed():
        st.session_state["on_changed_called_on"] = "experiments"

    with col1:
        st.subheader("Tables")
        selected_table = st.selectbox(
            "Tables", options=tables, on_change=tables_changed, key="selected_table"
        )

    with col2:
        st.subheader("Functions")
        selected_func = st.selectbox(
            "Functions", options=funcs, on_change=functions_changed, key="selected_func"
        )

    with col3:
        st.subheader("Models")
        selected_model = st.selectbox(
            "Models", options=models, on_change=models_changed, key="selected_model"
        )

    with col4:
        st.subheader("Experiments")
        selected_experiment = st.selectbox(
            "Experiments",
            options=experiments,
            on_change=experiments_changed,
            key="selected_experiment",
        )

    if "on_changed_called_on" in st.session_state:
        if st.session_state["on_changed_called_on"] == "tables":
            selected_table = st.session_state["selected_table"]
            if selected_table is not None:
                st.write(selected_table)
                st.table(
                    dask_sql_context.schema[selected_schema]
                    .tables[selected_table]
                    .df.head(SHOW_TABLE_ROWS)
                )

        elif st.session_state["on_changed_called_on"] == "func":
            selected_func = st.session_state["selected_func"]
            if selected_func is not None:
                st.write(selected_func)
                st.table(
                    pd.DataFrame(
                        dask_sql_context.schema[selected_schema].function_lists
                    ).astype(str)
                )
                funcObj = dask_sql_context.schema[selected_schema].functions[
                    selected_func
                ]
                # inspect will not work on the func where the func are defined are string
                # funcStr = inspect.getsource(funcObj)
                # st.code(funcStr)
                st.code(funcObj)

        elif st.session_state["on_changed_called_on"] == "models":
            model = st.session_state["selected_model"]
            if model is not None:
                st.write(f"Describe {model}")
                # st.write(dask_sql_context.schema[selected_schema].models[st.session_state['selected_model']])
                st.table(
                    dask_sql_context.sql(f"DESCRIBE MODEL {model}")
                    .compute()
                    .astype(str)
                )

        elif st.session_state["on_changed_called_on"] == "experiments":
            selected_experiment = st.session_state["selected_experiment"]
            if selected_experiment is not None:
                st.write(f"Describe Experiment {selected_experiment}")
                st.table(
                    dask_sql_context.schema[selected_schema]
                    .experiments[selected_experiment]
                    .astype(str)
                )

    with st.expander("HELP 💡"):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(
                """
                ##### Create Tables

                1. Refer Python Page for creating tables using dataframe
                2. Creating Table using SQL statement was not working
                without tunning in streamlit and coiled environment.
                3. But Works like charm when deployed locally because it
                able to access the data locally

                """
            )

        with col2:
            st.markdown(
                """
                ##### Create Python UDFs

                Refer Python page

                """
            )
        with col3:
            st.markdown(
                """
                ##### Create ML Models in SQL
                """
            )
            st.code(
                """

# Make sure the categorical columns
# are encoded before building model
# using when statement etc..
CREATE OR REPLACE MODEL my_model WITH (
    model_class = 'xgboost.dask.DaskXGBClassifier',
    target_column = 'species',
    num_class = 3
) AS (
    SELECT * FROM enriched_iris
)

                """,
                language="SQL",
            )
        with col4:
            st.markdown(
                """
                ##### Create ML Experiments using SQL

                1. Ability to tune Hyperparameter of the model
                2. AutoML like behaviour using TPOT

                usage: refer [dask-sql-docs](https://dask-sql.readthedocs.io/en/latest/pages/machine_learning.html)

                """
            )
