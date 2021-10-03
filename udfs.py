
import streamlit as st
from streamlit_ace import st_ace
from utils import get_settings_default
from connections import get_dask_sql_context

udf_help_text = "To more about how to define Python UDF, \
    please have a look at [dask-sql-docs](https://dask-sql.readthedocs.io/en/latest/pages/custom.html)"

dask_sql_context = get_dask_sql_context()

def register_udf(completefn,sql_context):
    try:
        import numpy as np
        import pandas as pd
        import dask.dataframe as dd
        cfn = compile(completefn,'kernel','exec')
        eval(cfn,{"__builtins__": {}},{"c":sql_context,"np":np,"pd":pd,"dd":dd})
    except ImportError as e:
        st.error(f"Import statements are minimized for security purpose! {e}")
    except Exception as e:
        st.error(e)

def create_pyUDF():
    theme,Keybinding,font_size,tab_size = get_settings_default(
            ["theme","Keybinding","font_size","tab_size"]
            )
    st.subheader("Write Python Function which can be used as UDF in SQL ")
    st.write("To more about how to define Python UDF, \
        please have a look at [dask-sql-docs](https://dask-sql.readthedocs.io/en/latest/pages/custom.html)")
    python_code = st_ace(
    placeholder="Write the Python Functions",
    language="python",
    theme=theme,
    keybinding=Keybinding,
    font_size=font_size,
    tab_size=tab_size,
    key="python_ace",
    )
    st.code(python_code,language="python")
    if python_code:
        register_udf(python_code,sql_context=dask_sql_context)
