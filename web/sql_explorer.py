import streamlit as st
from connections import get_dask_sql_context
from containers import get_last_few_queries, query_history
from defaults import SHOW_HISTORY_LENGTH, SHOW_TABLE_ROWS
from streamlit_ace import st_ace
from utils import get_settings_default


def write_sql():
    st.markdown(
        """
    ### Dask-SQL Workbench

    Use SQL to create Tables, Views,ML Models and Experiments etc.

    """
    )

    theme, Keybinding, font_size, tab_size = get_settings_default(
        ["theme", "Keybinding", "font_size", "tab_size"]
    )
    sql = st_ace(
        placeholder="Write the SQL statements",
        language="sql",
        theme=theme,
        keybinding=Keybinding,
        font_size=font_size,
        tab_size=tab_size,
        key="ace",
    )

    explain = st.button(
        label="Explain", help="Explain the Query plan to write a Optimized SQL"
    )
    dask_sql_context = get_dask_sql_context()

    dask_client = st.session_state.get("dask_client", None)

    if dask_client is None:
        st.sidebar.error(
            "Dask client should not be none, please initialize in connection page"
        )
    elif dask_client.status == "running":
        if sql:
            query_history.append(sql)

        if explain:
            if sql:
                st.code(dask_sql_context.explain(sql), language="text")

        if sql:
            sql_list = sql.split(";")
            for _sql in sql_list:
                _sql = _sql.strip()
                if _sql:
                    res = dask_sql_context.sql(_sql)
                    if res is not None:
                        st.table(res.head(SHOW_TABLE_ROWS).astype(str))
    else:
        st.write(
            f"Dask client was `{dask_client.status}` state, Need to be in `running` \
                 state to perform computation \
                 Please start/connect to the dask cluster in connection page"
        )

        st.sidebar.error(
            f"Dask client was in `{dask_client.status}` state, Need to be in `running` \
                    state to perform computation Please start/connect to the dask cluster in connection page"
        )

    with st.expander("Query History"):
        for sql in get_last_few_queries(n_rows=SHOW_HISTORY_LENGTH):
            st.code(sql, language="sql")

    with st.expander("HELP 💡"):
        st.subheader("Some SQL snippets")

        st.code(
            """
CREATE OR REPLACE TABLE enriched_iris AS (
    SELECT
        sepal_length, sepal_width, petal_length, petal_width,
        CASE
            WHEN species = 'setosa' THEN 0 ELSE CASE
            WHEN species = 'versicolor' THEN 1
            ELSE 2
        END END AS "species"
    FROM iris
)
            """,
            language="SQL",
        )

        st.subheader("Hyper parameter tunning")
        st.code(
            """
CREATE EXPERIMENT my_exp WITH (
    model_class = 'sklearn.ensemble.GradientBoostingClassifier',
    experiment_class = 'dask_ml.model_selection.GridSearchCV',
    tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                        max_depth = ARRAY [3,4,5,10]),
    target_column = 'species'
) AS (
        SELECT * FROM enriched_iris
    )
        """,
            language="SQL",
        )

        st.subheader("Create Table using dataset from S3 location")
        st.code(
            """
CREATE TABLE trip_data
    WITH (
        location = 's3://nyc-tlc/trip data/yellow_tripdata_2019-*.csv',
        format = 'csv',
        parse_dates = ARRAY ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
        dtype = MAP ['payment_type', 'UInt8',
                'VendorID', 'UInt8',
                'passenger_count', 'UInt8',
                'RatecodeID', 'UInt8',
                'store_and_fwd_flag', 'string',
                'PULocationID', 'UInt16',
                    'DOLocationID', 'UInt16'],
        storage_options= MAP ['anon','true'],
        blocksize='16 MiB'
    )
    """
        )
