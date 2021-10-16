import streamlit as st
from streamlit_ace import KEYBINDINGS, THEMES


def settings():
    # with st.sidebar.expander("⚙️ Settings"):
    # st.sidebar.subheader()
    st.markdown(
        """
    ### Settings Page

    Controls the asthetic of the editors (Python 🐍 and SQL IDE)
    """
    )

    theme = st.selectbox("Theme", options=THEMES, index=35)
    Keybinding = st.selectbox("Keybinding mode", options=KEYBINDINGS, index=3)

    if "font_size" in st.session_state:
        default_font_size = st.session_state["font_size"]
        font_size = st.slider("Font size", 14, 30, value=default_font_size)
    else:
        font_size = st.slider("Font size", 14, 30, value=24)

    if "tab_size" in st.session_state:
        default_tab_size = st.session_state["tab_size"]
        tab_size = st.slider("Tab size", 1, 8, default_tab_size)
    else:
        tab_size = st.slider("Tab size", 1, 8, 4)

    show_gutter = st.checkbox("Show gutter", value=True)
    show_print_margin = st.checkbox("Show print margin", value=False)
    wrap = st.checkbox("Wrap enabled", value=False)
    auto_update = st.checkbox("Auto update", value=False)
    readonly = st.checkbox("Read-only", value=False)
    # if 'theme' not in st.session_state:
    st.session_state["theme"] = theme
    st.session_state["Keybinding"] = Keybinding
    st.session_state["font_size"] = font_size
    st.session_state["tab_size"] = tab_size
