import streamlit as st
from streamlit_ace import THEMES,KEYBINDINGS

def get_settings():
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
    return theme,Keybinding,font_size,tab_size,show_gutter,show_print_margin,wrap,auto_update,readonly
