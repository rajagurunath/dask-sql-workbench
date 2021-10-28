import os
from functools import cache

import coiled
import dask
import streamlit as st
from containers import get_last_few_queries, query_history
from dask.distributed import Client, LocalCluster
from dask_sql import Context, java
from dask_sql.datacontainer import SchemaContainer

if "token" in os.environ:
    token = os.environ.get("token", None)
    dask.config.set({"coiled.token": token})


# @st.experimental_singleton()
# @st.cache(allow_output_mutation=True,ttl=3600)
def get_coiled_client():
    cluster = coiled.Cluster(
        n_workers=5, name="dask-sql-cluster", software="dask-sql-software"
    )
    client = Client(cluster)
    return client


# @st.experimental_singleton()
@st.cache(allow_output_mutation=True)
def get_dask_sql_context():
    # from dask.distributed import Client
    # client = Client()
    # print where it gets the class from. That should be the DaskSQL.jar
    print(
        java.org.codehaus.commons.compiler.CompilerFactoryFactory.class_.getProtectionDomain()
        .getCodeSource()
        .getLocation()
    )
    print(
        java.org.codehaus.commons.compiler.CompilerFactoryFactory.getDefaultCompilerFactory()
    )

    # print the JVM path, that should be your java installation
    print(java.jvmpath)

    client = get_dask_client(None)
    if client.status.lower() != "running":  # probably no this condition is not needed
        client.close()
        client = get_dask_client(None)

    return Context()


def coiled_dask_cluster():
    st.subheader("Scaling your cluster up or down")
    st.write(
        """
		We spun our Coiled Cluster up with 10 workers.
		You can scale this number up or down using the slider and button below.
		\n Note that scaling a cluster up takes a couple of minutes.
		"""
    )
    num_workers = st.slider("Number of workers", 5, 20, (5))
    if st.button("Scale your cluster!"):
        coiled.Cluster(name="dask-sql-cluster", software="dask-sql-software").scale(
            num_workers
        )
    # Option to shutdown cluster
    st.subheader("Cluster Hygiene")
    st.write(
        """
		To avoid incurring unnecessary costs, click the button below to shut down your cluster.
		Note that this means that a new cluster will have to be spun up the next time you run the app.
		"""
    )

    cluster_state = st.empty()

    cluster_state.write("Starting or connecting to Coiled cluster...")
    client = get_coiled_client()

    if client.status == "closed":
        # In a long-running Streamlit app, the cluster could have shut down from idleness.
        # If so, clear the Streamlit cache to restart it.

        dask_sql_context = get_dask_sql_context()
        dask_sql_context.schema_name = dask_sql_context.DEFAULT_SCHEMA_NAME
        dask_sql_context.schema = {
            dask_sql_context.schema_name: SchemaContainer(dask_sql_context.schema_name)
        }
        cluster_state.write("Starting or connecting to Coiled cluster...")
        client = get_coiled_client()

    if st.button("Shutdown Cluster"):
        st.write("This functionality may effect other users, Please handle with care")
        client.shutdown()

    cluster_state.write(
        f"Coiled cluster is up and Running..! ({client.dashboard_link})"
    )
    st.success("Dask cluster created successfully")
    st.balloons()
    return client


def connection_page():
    st.markdown(
        """
	### Creates Connection to Dask cluster

	1. Connect or Create to local Dask Cluster (was disabled in streamlit share)
	2. Connect or Create to Coiled Dask Cluster
	"""
    )
    local_or_coiled = st.radio(
        "Do you want to connect to local \
								 dask cluster or coiled dask cluster",
        options=["local-reuse", "local-create", "coiled-create"],
        key="local_or_coiled",
    )

    dask_client = get_dask_client(local_or_coiled)


def get_dask_client(local_or_coiled=None):
    if local_or_coiled is None:
        local_or_coiled = st.session_state.get("local_or_coiled", "coiled-create")
    deployment_type = os.environ.get("deployment_type", None)
    if local_or_coiled in ["local-reuse", "local-create"]:
        if deployment_type == "streamlit":
            raise st.StreamlitAPIException(
                "App was deployed in streamlit, for \
			performance reason local dask clusters cannot be created,Please use Coiled Dask Envrionment"
            )

        client = local_dask_cluster(local_or_coiled)
    else:
        client = coiled_dask_cluster()
        st.write("coiled cluster creation ...")
    st.session_state["dask_client"] = client
    return client


@st.experimental_singleton()
def _create_local_client(num_workers):
    lcluster = LocalCluster(n_workers=num_workers, threads_per_worker=4)
    client = Client(lcluster)
    return client


@st.experimental_singleton()
def _resuse_local_client(scheduler_addr):
    client = Client(scheduler_addr)
    return client


def local_dask_cluster(action):
    st.write(action)
    if action == "local-create":
        num_workers = st.slider("Number of workers", 5, 20, (6))

        if st.button("Create Local Cluster"):
            client = _create_local_client(num_workers)
            st.write(f"Created Local Dask Cluster:({client.dashboard_link})")
            return client

    elif action == "local-reuse":
        # reuse existing cluster
        dask_scheduler_address = st.text_input(
            "dask scheduler address", value="tcp://localhost:8786"
        )
        if st.button("Reuse Cluster"):
            client = _resuse_local_client(dask_scheduler_address)
            st.write(f"Reusing Local Dask Cluster:({client.dashboard_link})")
            return client
    else:
        raise Exception("I know this will not happen")
