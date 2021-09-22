import streamlit as st
import dask
import coiled


@st.cache(allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
def get_client():
	cluster_state.write("Starting or connecting to Coiled cluster...")
	dask.config.set({"coiled.token":st.secrets['token']})

	cluster = coiled.Cluster(
		n_workers=10,
		name="coiled-streamlit",
		software="coiled-examples/streamlit",
	)

	client = Client(cluster)
	return client