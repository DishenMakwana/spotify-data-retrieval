import streamlit as st

st.set_page_config(layout="wide")

# Sidebar Navigation - Home Button (Appears on Other Pages)
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "HOME"  # Default to HOME page

if st.session_state["current_page"] != "HOME":
    with st.sidebar:
        if st.button("🏠 HOME"):
            st.session_state["current_page"] = "HOME"
            st.switch_page("frontend.py")  # Change to your home page file name

# Title & Instructions
st.title("Music Library Dashboard")
st.write("Welcome! Use the buttons below to navigate between pages.")

# Navigation Buttons
navigation_items = {
    "user recent played songs": "pages/user_recent_played_songs.py",
    "tracks": "pages/tracks.py",
    "albums": "pages/albums.py",
    "artists": "pages/artists.py"
}

col1, col2 = st.columns(2)

for index, (label, page) in enumerate(navigation_items.items()):
    button_label = label.upper()  # Convert to uppercase
    with (col1 if index % 2 == 0 else col2):
        if st.button(button_label):
            st.session_state["current_page"] = label.upper()  # Track the current page
            st.switch_page(page)
