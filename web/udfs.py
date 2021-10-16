import streamlit as st
from connections import get_dask_sql_context
from streamlit_ace import st_ace
from utils import get_settings_default

udf_help_text = "To more about how to define Python UDF, \
    please have a look at [dask-sql-docs](https://dask-sql.readthedocs.io/en/latest/pages/custom.html)"

dask_sql_context = get_dask_sql_context()


def register_udf(completefn, sql_context):
    try:
        import dask.dataframe as dd
        import numpy as np
        import pandas as pd

        cfn = compile(completefn, "kernel", "exec")
        eval(
            cfn, {"__builtins__": {}}, {"c": sql_context, "np": np, "pd": pd, "dd": dd}
        )
    except ImportError as e:
        st.error(f"Import statements are minimized for security purpose! {e}")
    except Exception as e:
        st.error(e)


def create_pyUDF():
    st.markdown(
        """
        ### This is Python IDE used for following purposes

        1. create SQL UDF

        2. As a Side effect (trick) this IDE can be used to import Custom data files
            example: CSVs stored in github, HTML tables from any webpage using pandas

        you will have access to modules such as

        1. pandas as pd
        2. numpy as np
        3. dask.dataframe as dd
        4. dask_sql_context as c (which is used to register the python func as custom SQL UDFs)
        """
    )
    theme, Keybinding, font_size, tab_size = get_settings_default(
        ["theme", "Keybinding", "font_size", "tab_size"]
    )
    st.subheader("Write Python Function which can be used as UDF in SQL ")
    st.write(
        "To more about how to define Python UDF, \
        please have a look at [dask-sql-docs](https://dask-sql.readthedocs.io/en/latest/pages/custom.html)"
    )
    python_code = st_ace(
        placeholder="Write the Python Functions",
        language="python",
        theme=theme,
        keybinding=Keybinding,
        font_size=font_size,
        tab_size=tab_size,
        key="python_ace",
    )
    st.code(python_code, language="python")
    dask_client = st.session_state.get("dask_client", None)

    if dask_client is None:
        st.sidebar.error(
            "Dask client should not be none, please initialize in connection page"
        )
    elif dask_client.status == "running":
        if python_code:
            register_udf(python_code, sql_context=dask_sql_context)
    else:
        st.write(
            f"Dask client was `{dask_client.status}`, Need to be in `running` \
                 state to perform computation \
                 Please start/connect to the dask cluster in connection page"
        )

        st.sidebar.error(
            f"Dask client was in `{dask_client.status}`, Need to be in `running` \
                    state perform computation Please start/connect to the dask cluster in connection page"
        )

    with st.expander("HELP 💡"):
        st.subheader("Example snippets for UDFs Creation")
        st.code(
            """
def volume(length, width):
    return (width / 2) ** 2 * np.pi * length

# As SQL is a typed language, we need to specify all types
c.register_function(volume, "IRIS_VOLUME",
                    parameters=[("length", np.float64), ("width", np.float64)],
                    return_type=np.float64)
            """
        )

        st.subheader(
            "Example snippet for importing data from S3 and Registering it as Table"
        )
        st.code(
            """
df = dd.read_csv(
    "s3://nyc-tlc/trip data/yellow_tripdata_2019-*.csv",
    parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    dtype={
        "payment_type": "UInt8",
        "VendorID": "UInt8",
        "passenger_count": "UInt8",
        "RatecodeID": "UInt8",
        "store_and_fwd_flag": "string",
        "PULocationID": "UInt16",
        "DOLocationID": "UInt16",
    },
    storage_options={"anon": True},
    blocksize="16 MiB",
).persist()


c.create_table("trip_data",df)

            """
        )
        st.subheader(
            "Example snippet for importing data from github and Registering it as Table"
        )
        st.code(
            """
df = pd.read_csv("https://raw.githubusercontent.com/rajagurunath/dask-sql-workbench/main/iris.csv")
c.create_table("iris",df)
            """
        )
