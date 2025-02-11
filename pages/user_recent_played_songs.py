import streamlit as st
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

st.title("User Recent Played Songs")

API_URL = os.getenv("API_URL").strip() + "/user_tracks/"

if "page" not in st.session_state:
    st.session_state["page"] = 1
if "page_size" not in st.session_state:
    st.session_state["page_size"] = 10  # Default page size

def change_page(new_page):
    st.session_state["page"] = new_page
    st.rerun()

def change_page_size(new_size):
    st.session_state["page_size"] = new_size
    st.session_state["page"] = 1  # Reset to first page
    st.rerun()

params = {"page": st.session_state["page"], "page_size": st.session_state["page_size"]}
response = requests.get(API_URL, params=params)

if response.status_code == 200:
    result = response.json()
    data = result.get("data", [])
    total_records = data.get("total", 0)

    if data:
        data = data.get("history", [])

        df = pd.DataFrame(data)

        if {"album_id", "album_name", "album_url"}.issubset(df.columns):
            df["Album Link"] = df["album_url"].apply(lambda x: f'<a href="{x}" target="_blank">Open</a>')
            df["Track Url Link"] = df["track_url"].apply(lambda x: f'<a href="{x}" target="_blank">Open</a>')
            df["Playlist Url Link"] = df["context_url"].apply(lambda x: f'<a href="{x}" target="_blank">Open</a>')
            df["Album Image"] = df["track_album_image"].apply(lambda x: f'<img src="{x if x else ""}" style="width:100px; height:100px;">')

            df["played_at"] = pd.to_datetime(df["played_at"]).dt.strftime('%d-%m-%Y %I:%M %p')

            cols = ["album_id", "album_name", "Album Link", "Album Image"] + [col for col in df.columns if col not in ["album_id", "album_name", "album_url", "Album Link", "Album Image"]]
            df = df[cols]

            df = df.drop(columns=["track_url", "context_url", "track_album_artists", "track_album_images", "track_album_image"])

            df.columns = df.columns.str.replace("_", " ").str.title()

            # 🔹 **Set Fixed Column Widths with CSS**
            st.markdown("""
                <style>
                table { width: 100%; border-collapse: collapse; }
                th, td { padding: 10px; text-align: left; white-space: nowrap; }
                td img { width: 80px; height: 80px; object-fit: cover; }
                td:nth-child(1) { width: 100px; }  /* Album ID */
                td:nth-child(2) { width: 200px; }  /* Album Name */
                td:nth-child(3) { width: 120px; }  /* Navigate */
                td:nth-child(4) { width: 120px; }  /* Album Image */
                td:nth-child(5) { width: 150px; }  /* Played At */
                </style>
            """, unsafe_allow_html=True)

            st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.dataframe(df, use_container_width=True)
    else:
        st.write("No records found.")

    total_pages = (total_records + st.session_state["page_size"] - 1) // st.session_state["page_size"]

    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])

    with col2:
        if st.session_state["page"] > 1:
            if st.button("Previous"):
                change_page(st.session_state["page"] - 1)

    with col3:
        if st.session_state["page"] < total_pages:
            if st.button("Next"):
                change_page(st.session_state["page"] + 1)

    with col4:
        new_size = st.selectbox("Page Size", [5, 10, 20, 50], index=[5, 10, 20, 50].index(st.session_state["page_size"]), label_visibility="collapsed", key="page_size_selector")
        if new_size != st.session_state["page_size"]:
            change_page_size(new_size)
else:
    st.error("No records found.")
