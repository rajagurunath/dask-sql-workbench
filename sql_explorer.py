from streamlit_ace import st_ace
import streamlit as st
from utils import get_settings_default
from containers import get_last_few_queries,query_history
from connections import get_dask_sql_context
from defaults import SHOW_HISTORY_LENGTH,SHOW_TABLE_ROWS

dask_sql_context = get_dask_sql_context()


def write_sql():
    theme,Keybinding,font_size,tab_size = get_settings_default(
            ["theme","Keybinding","font_size","tab_size"]
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

    explain = st.button(label="Explain")

    if sql:
        query_history.append(sql)

    if explain:
        if sql:
            st.code(dask_sql_context.explain(sql),language="text")

    if sql:
        sql_list = sql.split(";")
        for _sql in sql_list:
            _sql = _sql.strip()
            if _sql:
                res = dask_sql_context.sql(_sql)
                if res is not None:
                    st.table(res.head(SHOW_TABLE_ROWS).astype(str))

    with st.expander("Query History"):
        for sql in get_last_few_queries(n_rows=SHOW_HISTORY_LENGTH):
            st.code(sql,language="sql")
