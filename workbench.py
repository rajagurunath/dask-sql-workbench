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
st.sidebar.title("Dask-SQL")
with st.sidebar.expander("🔌 Connection"):
    st.write("<TO BE FILLED>")
# st.sidebar.selectbox()

st.sidebar.subheader("Schemas")


for idx,(schema_name,schema) in enumerate(dask_sql_context.schema.items()):
    with st.sidebar.expander(schema_name):
        # st.sidebar.header(schema_name)
        st.subheader("Tables")
        choosed_table = st.selectbox(label="Tables",options=schema.tables,
                                help=f"select the Tables of the schema {schema_name}",
                                args=schema_name,
                                kwargs={"schema":schema_name},
                                key=f"{idx}_Tables")
        st.subheader("Functions")
        choosed_fn = st.selectbox(label="Functions",options=schema.functions,
                                    key=f"{idx}_Functions")
        st.subheader("Experiments")
        choosed_exp = st.selectbox(label="Experiments",options=schema.experiments,
                                    key=f"{idx}_Experiments")
        st.subheader("Models")
        choosed_model = st.selectbox(label="Models",options=schema.models,
                                    key=f"{idx}_Models")



create_udf_function = st.sidebar.radio("Create Python UDF",options=("No","Yes"),help=udf_help_text)

with st.sidebar.expander("Visualizer"): 
    chart_type = st.selectbox("chart Types",options=["line","bar"])



with st.sidebar.expander("⚙️ Settings"):
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
    # st.write(sql)
    st.code(sql,language="sql")
    sql_list = sql.split(";")
    for _sql in sql_list:
        res = dask_sql_context.sql(_sql)
    if res is not None:
        st.table(res.compute().head(100))


with st.expander("Visualizer"):
    if res is not None:
        if chart_type =="bar":
            st.bar_chart(res)
        elif chart_type == "line":
            st.line_chart(res)


with st.expander("Query History"):
    for sql in get_last_few_queries(n_rows=15):
        st.code(sql,language="sql")


if create_udf_function == "Yes":
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
    register_udf(python_code,sql_context=dask_sql_context)
