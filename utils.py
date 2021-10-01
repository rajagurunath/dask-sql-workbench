import streamlit as st
from defaults import DEFAULT_SETTINGS
import coiled

def _get_keys_from_session(key):
    if key in st.session_state:
        return st.session_state[key]
    return None

def get_settings_default(keys_required):
    required_values = []
    for key in keys_required:
        value = _get_keys_from_session(key)
        if value is None:
            value = DEFAULT_SETTINGS[key]
        required_values.append(value)
    return required_values

def create_required_softwares(packages):
    coiled.create_software_environment(
    name="dask-sql-software",
    pip=["dask", "xgboost", "dask-sql","dask-ml","tpot"],
    )   