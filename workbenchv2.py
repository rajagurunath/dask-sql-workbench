import dask_sql
import streamlit as st
from dask_sql import Context
from streamlit.state.session_state import WidgetArgs
from streamlit_ace import st_ace, THEMES,KEYBINDINGS
from dask_sql import java
from containers import get_last_few_queries,query_history
from udfs import register_udf,udf_help_text
# from settings import get_settings
st.set_page_config(layout="wide")

global theme,Keybinding,font_size,tab_size
@st.cache(allow_output_mutation=True)
def get_dask_sql_context():
    # from dask.distributed import Client
    # client = Client()
    # print where it gets the class from. That should be the DaskSQL.jar
    print(java.org.codehaus.commons.compiler.CompilerFactoryFactory.class_.getProtectionDomain().getCodeSource().getLocation())
    print(java.org.codehaus.commons.compiler.CompilerFactoryFactory.getDefaultCompilerFactory())

    # print the JVM path, that should be your java installation
    print(java.jvmpath)

    return Context()


dask_sql_context = get_dask_sql_context()

user_want_to= st.sidebar.radio(
    "Dask-SQL",
    options= [
        "Write SQL",
        "Explore Schema",
        "Connections",
        "Settings"

    ]

)

def settings():
    # with st.sidebar.expander("⚙️ Settings"):
    # st.sidebar.subheader()
    theme = st.selectbox("Theme", options=THEMES, index=35)
    Keybinding = st.selectbox("Keybinding mode", options=KEYBINDINGS, index=3)
    font_size= st.slider("Font size", 14, 30,value=24)
    tab_size = st.slider("Tab size", 1, 8, 4)
    show_gutter=st.checkbox("Show gutter", value=True)
    show_print_margin=st.checkbox("Show print margin", value=False)
    wrap=st.checkbox("Wrap enabled", value=False)
    auto_update=st.checkbox("Auto update", value=False)
    readonly=st.checkbox("Read-only", value=False)
    return theme,Keybinding,font_size,tab_size

def write_sql():
    global theme,Keybinding,font_size,tab_size
    sql = st_ace(
    placeholder="Write the SQL statements",
    language="sql",
    theme=THEMES[35],
    keybinding=KEYBINDINGS[3],
    font_size=24,
    tab_size=4,
    
    key="ace",
    )

    explain = st.button(label="Explain")


if user_want_to == "Write SQL":
    write_sql()
elif user_want_to == "Settings":
    theme,Keybinding,font_size,tab_size = settings()
