import streamlit as st

st.set_page_config(layout="wide")

# Title & Instructions
st.title("Music Library Dashboard")
st.write("Welcome! Use the buttons below to navigate between pages.")

# Navigation Buttons
navigation_items = {
    "User recent played songs": "pages/user_recent_played_songs.py",
    "Tracks": "pages/tracks.py",
    "Albums": "pages/albums.py",
    "Artists": "pages/artists.py"
}

col1, col2 = st.columns(2)

for index, (label, page) in enumerate(navigation_items.items()):
    button_label = label 
    with (col1 if index % 2 == 0 else col2):
        if st.button(button_label):
            st.session_state["current_page"] = label
            st.switch_page(page)
