import pandas as pd
import streamlit as st
from pandas_profiling import ProfileReport
from streamlit_ace import st_ace
from streamlit_pandas_profiling import st_profile_report

from connections import get_dask_sql_context
from utils import get_settings_default

dask_sql_context = get_dask_sql_context()


def code_visualizer(completefn, sql_context, table):
    import matplotlib.pyplot as plt
    import seaborn as sns

    cfn = compile(completefn, "string", "eval")
    st.write(cfn)
    st.write(
        eval(
            cfn,
            {"__builtins__": {}},
            {"c": sql_context, "plt": plt, "sns": sns, "df": table},
        ).figure
    )


def visualize_table(table):
    st.subheader("Custom Visualization")
    st.write("Steps to visualize data")
    st.markdown(
        """

    1. choose required table to visualize.

    2. The choosed table will be available as the variable df.

    3. Write visualization code using seaborn and pandas dataframe visualizer.

    4. Charts will be displayed if there is no syntax error

    5. Not all visualization features will work at the moment (:

    """
    )
    theme, Keybinding, font_size, tab_size = get_settings_default(
        ["theme", "Keybinding", "font_size", "tab_size"]
    )
    python_code = st_ace(
        placeholder="Write the Visualization code",
        language="python",
        theme=theme,
        keybinding=Keybinding,
        font_size=font_size,
        tab_size=tab_size,
        key="python_ace",
    )
    st.code(python_code, language="python")
    if python_code:
        st.write(code_visualizer(python_code, dask_sql_context, table))


def visualizer_page():

    st.header("Visualize your Data ")
    st.caption("A Poor man's Visualization Tool")
    schemas = dask_sql_context.schema.keys()
    selected_schema = st.selectbox("Schemas", options=schemas,)

    tables = dask_sql_context.schema[selected_schema].tables
    st.subheader("Tables")
    selected_table = st.selectbox("Tables", options=tables)

    viz_type = st.selectbox(
        "What you want to do with the data ?", options=["custom viz", "profile"]
    )

    if selected_table is None:
        st.warning("No Tables Detected, Please create/select one to proceed further")
    else:
        dataframe = (
            dask_sql_context.schema[selected_schema]
            .tables[selected_table]
            .df.head(1000)
        )

        if viz_type == "profile":
            pr = ProfileReport(dataframe)  # .profile_report()
            st_profile_report(pr)
        elif viz_type == "custom viz":
            visualize_table(dataframe)
        else:
            raise Exception("This should not happen !!!")

    with st.expander("HELP 💡"):
        st.subheader("Example Custom Viz snippets")
        st.code(
            """
sns.scatterplot(
    data=df,
    x="sepal_length",
    y="sepal_width",
    hue="species",
)
            """
        )
