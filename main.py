import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect, text
import psycopg2
from dotenv import load_dotenv
import json

load_dotenv()

# 🔹 PostgreSQL Database Connection
DATABASE_URL = os.getenv("DATABASE_URL")
schema_name = os.getenv("SCHEMA_NAME")
engine = create_engine(DATABASE_URL)

# 🔹 Spotify API Credentials
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")

# print("DATABASE_URL:", DATABASE_URL)
# print("SPOTIFY_CLIENT_ID:", SPOTIFY_CLIENT_ID)
# print("SPOTIFY_CLIENT_SECRET:", SPOTIFY_CLIENT_SECRET)
# print("SPOTIFY_REDIRECT_URI:", SPOTIFY_REDIRECT_URI)

# 🔹 Spotify Authentication
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET,
    redirect_uri=SPOTIFY_REDIRECT_URI,
    scope="user-read-recently-played user-read-email user-read-private playlist-read-private playlist-read-collaborative user-follow-read user-top-read user-library-read"
))

# Function to Read Data from SQL
def read_from_sql(table_name):
    """
    Read data from a PostgreSQL table and return as a pandas DataFrame.

    Parameters:
    - table_name (str): Name of the table in the database.
    - schema_name (str): The schema of the table in the database.

    Returns:
    - pd.DataFrame: The query result.
    """
    try:
        print(f"Reading data from table '{schema_name}.{table_name}'...")
        query = f"SELECT * FROM {schema_name}.{table_name};"
        dataframe = pd.read_sql(query, con=engine)
        return dataframe
    except Exception as e:
        print(f"Error reading from SQL: {e}")
        return None

# Function to Write Data to SQL
def write_to_sql(dataframe, table_name, if_exists="append"):
    """
    Write a pandas DataFrame to a PostgreSQL table.

    Parameters:
    - dataframe (pd.DataFrame): The data to write.
    - table_name (str): The name of the target SQL table.
    - if_exists (str): {"fail", "replace", "append"} behavior for existing tables.
    - schema_name (str): The schema of the table to write to.
    """
    try:
        # Convert lists/dictionaries to strings
        for column in dataframe.columns:
            if dataframe[column].apply(lambda x: isinstance(x, (list, dict))).any():
                dataframe[column] = dataframe[column].apply(str)

        # Inspect the table structure with the schema
        inspector = inspect(engine)
        if f"{schema_name}.{table_name}" in inspector.get_table_names():
            print(f"✅ Table '{schema_name}.{table_name}' exists. Checking for new columns...")
            existing_columns = [col["name"] for col in inspector.get_columns(f"{schema_name}.{table_name}")]
            for column in dataframe.columns:
                if column not in existing_columns:
                    print(f"➕ Adding new column '{column}' to table '{schema_name}.{table_name}'...")
                    with engine.connect() as connection:
                        connection.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {column} TEXT NULL"))
        else:
            print(f"🚀 Table '{schema_name}.{table_name}' does not exist. It will be created.")

        # Write to SQL (specify schema)
        dataframe.to_sql(table_name, con=engine, if_exists=if_exists, index=False, schema=schema_name)
        print(f"✅ Data successfully written to table '{schema_name}.{table_name}'")
    except Exception as e:
        print(f"⚠️ Error writing to SQL: {e}")

def delete_from_sql(table_name, sqlQuery=None):
    """
    Delete data from a SQL database, with schema consideration.

    Parameters:
    - table_name: str, the name of the SQL table.
    - sqlQuery: str, custom SQL query to run (optional).
    """
    try:
        print(f"Deleting data from table '{schema_name}.{table_name}'...")

        # Check if table exists in the schema
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names(schema=schema_name):
            print(f"Table '{schema_name}.{table_name}' does not exist.")
            return

        query = sqlQuery if sqlQuery else f"DELETE FROM {schema_name}.{table_name}"

        # Execute the SQL query with explicit commit
        with engine.connect() as connection:
            transaction = connection.begin()  # Start a transaction
            try:
                result = connection.execute(text(query))
                transaction.commit()  # Explicitly commit the transaction
                print(f"Data successfully deleted from table '{schema_name}.{table_name}'")
            except Exception as e:
                transaction.rollback()  # Roll back if something goes wrong
                print(f"An exception occurred: {str(e)}")

    except Exception as e:
        print(f"An exception occurred: {str(e)}")
        return None

# 🔹 Extract Data from Spotify API (Last 24 Hours)
def extract_spotify_data():
    """
    Extract recently played tracks from Spotify API (last 24 hours).
    Handles pagination to fetch all available tracks.
    """
    now = datetime.utcnow()
    yesterday = now - timedelta(days=1)
    yesterday_unix = int(yesterday.timestamp() * 1000)

    print("🔄 Fetching recently played songs from Spotify...")

    results = sp.current_user_recently_played(limit=50, after=yesterday_unix)
    all_tracks = results["items"]  # Initialize with the first page's tracks

    # Loop through pages of results
    while results.get("next"):
        print("➡️ Fetching next page of current_user_recently_played results...")
        results = sp.next(results)  # Get the next page of results
        all_tracks.extend(results["items"])  # Add new tracks to the list

    # Check if any tracks were found
    if all_tracks:
        df = pd.json_normalize(all_tracks)  # Flatten the JSON response into a DataFrame
        return df
    else:
        print("⚠️ No recent tracks found!")
        return pd.DataFrame()  # Return an empty DataFrame if no data


# 🔹 Run ETL Process
def fetch_user_tracks_history():
    print("🚀 Running ETL Process...")

    # 1️⃣ Extract
    df_spotify = extract_spotify_data()
    if df_spotify.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_spotify.columns = df_spotify.columns.str.replace('.', '_')

    # print(df_spotify.columns)

    # 3️⃣ Load to Database
    write_to_sql(df_spotify, "user_tracks_history", if_exists="append")
    # df_spotify.to_csv("spotify_tracks.csv", index=False, encoding="utf-8")

def format_user_tracks_history():
    # Read data from SQL
    df = read_from_sql("user_tracks_history")
    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Convert played_at to datetime
    df["played_at"] = pd.to_datetime(df["played_at"])

    # Extract date and time
    df["played_date"] = df["played_at"].dt.date
    df["played_time"] = df["played_at"].dt.time

    # add new column
    # Convert the JSON string to a Python object (list of dictionaries)
    df["track_album_artists"] = df["track_album_artists"].apply(lambda x: json.loads(x.replace("'", '"')) if isinstance(x, str) else x)

    # Extract the 'name' and 'id' fields
    df["album_artists_name"] = df["track_album_artists"].apply(lambda x: x[0]["name"] if isinstance(x, list) and isinstance(x[0], dict) else None)
    df["album_artists_id"] = df["track_album_artists"].apply(lambda x: x[0]["id"] if isinstance(x, list) and isinstance(x[0], dict) else None)

    # select only necessary columns
    df = df[["played_at", "track_album_album_type", "track_album_external_urls_spotify", "track_album_id", "track_album_name", "track_album_release_date", "track_duration_ms", "track_id", "track_name", "track_popularity", "track_external_urls_spotify", "context_external_urls_spotify", "played_date", "played_time", "album_artists_name", "album_artists_id"]]

    # rename columns
    df = df.rename(columns={
        "track_album_album_type": "album_type",
        "track_album_external_urls_spotify": "album_url",
        "track_album_id": "album_id",
        "track_album_name": "album_name",
        "track_album_release_date": "album_release_date",
        "track_duration_ms": "duration_ms",
        "track_external_urls_spotify": "track_url",
        "context_external_urls_spotify": "context_url",
    })

    # write to sql
    write_to_sql(df, "user_tracks_history_formatted", if_exists="replace")
    print("🎉 Data formatted successfully!")

def fetch_album_data_for_user_tracks():
    # Read data from SQL
    df = read_from_sql("user_tracks_history_formatted")
    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique album IDs
    unique_album_ids = df["album_id"].unique()

    # Fetch album data from Spotify API with pagination
    album_data = []
    for album_id in unique_album_ids:
        try:
            print(f"🔄 Fetching album data for ID {album_id}...")
            result = sp.album(album_id)
            album_data.append(result)

            # Check if there are more pages of album data (if applicable)
            while result.get("next"):
                print("➡️ Fetching next page of album data...")
                result = sp.next(result)
                album_data.append(result)

        except Exception as e:
            print(f"⚠️ Error fetching album data for ID {album_id}: {e}")

    # Flatten JSON response
    df_albums = pd.json_normalize(album_data)

    # Convert lists/dictionaries to strings
    for column in df_albums.columns:
        if df_albums[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_albums[column] = df_albums[column].apply(str)

    # Write to SQL
    write_to_sql(df_albums, "album_data", if_exists="replace")
    print("🎉 Album data fetched successfully!")

def fetch_track_data_for_user_tracks():
    # Read data from SQL
    df = read_from_sql("user_tracks_history_formatted")
    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique track IDs
    unique_track_ids = df["track_id"].unique()

    # Fetch track data from Spotify API with pagination
    track_data = []
    for track_id in unique_track_ids:
        try:
            print(f"🔄 Fetching track data for ID {track_id}...")
            result = sp.track(track_id)
            track_data.append(result)

            # Check if there are more pages of track data (if applicable)
            while result.get("next"):
                print("➡️ Fetching next page of track data...")
                result = sp.next(result)
                track_data.append(result)

        except Exception as e:
            print(f"⚠️ Error fetching track data for ID {track_id}: {e}")

    # Flatten JSON response
    df_tracks = pd.json_normalize(track_data)

    # Convert lists/dictionaries to strings
    for column in df_tracks.columns:
        if df_tracks[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_tracks[column] = df_tracks[column].apply(str)

    # Write to SQL
    write_to_sql(df_tracks, "track_data", if_exists="replace")
    print("🎉 Track data fetched successfully!")

def fetch_artist_data_for_user_tracks():
    # Read data from SQL
    df = read_from_sql("user_tracks_history_formatted")
    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique artist IDs
    unique_artist_ids = df["album_artists_id"].unique()

    # Fetch artist data from Spotify API with pagination
    artist_data = []
    for artist_id in unique_artist_ids:
        try:
            print(f"🔄 Fetching artist data for ID {artist_id}...")
            result = sp.artist(artist_id)
            artist_data.append(result)

            # Check if there are more pages of artist data (if applicable)
            while result.get("next"):
                print("➡️ Fetching next page of artist data...")
                result = sp.next(result)
                artist_data.append(result)

        except Exception as e:
            print(f"⚠️ Error fetching artist data for ID {artist_id}: {e}")

    # Flatten JSON response
    df_artists = pd.json_normalize(artist_data)

    # Convert lists/dictionaries to strings
    for column in df_artists.columns:
        if df_artists[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_artists[column] = df_artists[column].apply(str)

    # Write to SQL
    write_to_sql(df_artists, "artist_data", if_exists="replace")
    print("🎉 Artist data fetched successfully!")

def fetch_user_followed_artists():
    # Fetch followed artists from Spotify API with pagination
    followed_artists = []
    results = sp.current_user_followed_artists(limit=50)

    # Loop through pages of results
    while results:
        followed_artists.extend(results["artists"]["items"])  # Add new artists to the list

        # Check if there are more pages of artist data (if applicable)
        if results["artists"]["next"]:
            print("➡️ Fetching next page of followed artists...")
            results = sp.next(results["artists"])
        else:
            results = None

    # Flatten JSON response
    df_followed_artists = pd.json_normalize(followed_artists)

    # Convert lists/dictionaries to strings
    for column in df_followed_artists.columns:
        if df_followed_artists[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_followed_artists[column] = df_followed_artists[column].apply(str)

    # Write to SQL
    write_to_sql(df_followed_artists, "user_followed_artists", if_exists="replace")
    print("🎉 Followed artists fetched successfully!")

def fetch_user_playlists():
    # Fetch user playlists from Spotify API with pagination
    user_playlists = []
    results = sp.current_user_playlists(limit=50)

    # Loop through pages of results
    while results:
        user_playlists.extend(results["items"])  # Add new playlists to the list

        # Check if there are more pages of playlist data (if applicable)
        if results["next"]:
            print("➡️ Fetching next page of user playlists...")
            results = sp.next(results)
        else:
            results = None

    # Flatten JSON response
    df_user_playlists = pd.json_normalize(user_playlists)

    # Convert lists/dictionaries to strings
    for column in df_user_playlists.columns:
        if df_user_playlists[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_user_playlists[column] = df_user_playlists[column].apply(str)

    # Write to SQL
    write_to_sql(df_user_playlists, "user_playlists", if_exists="replace")
    print("🎉 User playlists fetched successfully!")

def fetch_artist_top_tracks():
    # Read data from SQL
    df = read_from_sql("artist_data")
    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique artist IDs
    unique_artist_ids = df["id"].unique()

    # Fetch top tracks for each artist
    top_tracks = []
    for artist_id in unique_artist_ids:
        try:
            print(f"🔄 Fetching top tracks for artist ID {artist_id}...")
            result = sp.artist_top_tracks(artist_id)
            top_tracks.append(result)

        except Exception as e:
            print(f"⚠️ Error fetching top tracks for artist ID {artist_id}: {e}")

    # Flatten JSON response
    df_top_tracks = pd.json_normalize(top_tracks)

    # Convert lists/dictionaries to strings
    for column in df_top_tracks.columns:
        if df_top_tracks[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_top_tracks[column] = df_top_tracks[column].apply(str)

    # Write to SQL
    write_to_sql(df_top_tracks, "artist_top_tracks", if_exists="replace")
    print("🎉 Top tracks fetched successfully!")

def fetch_artist_related_artists():
    # Read data from SQL
    df = read_from_sql("artist_data")
    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique artist IDs
    unique_artist_ids = df["id"].unique()

    # Fetch related artists for each artist
    related_artists = []
    for artist_id in unique_artist_ids:
        try:
            print(f"🔄 Fetching related artists for artist ID {artist_id}...")
            result = sp.artist_related_artists(artist_id)
            related_artists.append(result)

        except Exception as e:
            print(f"⚠️ Error fetching related artists for artist ID {artist_id}: {e}")

    # Flatten JSON response
    df_related_artists = pd.json_normalize(related_artists)

    # Convert lists/dictionaries to strings
    for column in df_related_artists.columns:
        if df_related_artists[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_related_artists[column] = df_related_artists[column].apply(str)

    # Write to SQL
    write_to_sql(df_related_artists, "artist_related_artists", if_exists="replace")
    print("🎉 Related artists fetched successfully!")

def fetch_user_saved_albums():
    # Fetch saved albums from Spotify API with pagination
    saved_albums = []
    results = sp.current_user_saved_albums(limit=50)

    # Loop through pages of results
    while results:
        saved_albums.extend(results["items"])  # Add new albums to the list

        # Check if there are more pages of album data (if applicable)
        if results["next"]:
            print("➡️ Fetching next page of saved albums...")
            results = sp.next(results)
        else:
            results = None

    # Flatten JSON response
    df_saved_albums = pd.json_normalize(saved_albums)

    # Convert lists/dictionaries to strings
    for column in df_saved_albums.columns:
        if df_saved_albums[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_saved_albums[column] = df_saved_albums[column].apply(str)

    # Write to SQL
    write_to_sql(df_saved_albums, "user_saved_albums", if_exists="replace")
    print("🎉 Saved albums fetched successfully!")

def get_new_releases_albums():
    # Fetch new releases from Spotify API with pagination
    new_releases = []
    results = sp.new_releases(limit=50)

    # Loop through pages of results
    while results:
        new_releases.extend(results["albums"]["items"])  # Add new albums to the list

        # Check if there are more pages of album data (if applicable)
        if results["albums"]["next"]:
            print("➡️ Fetching next page of new releases...")
            results = sp.next(results["albums"])
        else:
            results = None

    # Flatten JSON response
    df_new_releases = pd.json_normalize(new_releases)

    # Convert lists/dictionaries to strings
    for column in df_new_releases.columns:
        if df_new_releases[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_new_releases[column] = df_new_releases[column].apply(str)

    # Write to SQL
    write_to_sql(df_new_releases, "new_releases_albums", if_exists="replace")
    print("🎉 New releases fetched successfully!")

def fetch_playlist_items():
    # Read data from SQL
    df = read_from_sql("user_playlists")
    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique playlist IDs
    unique_playlist_ids = df["id"].unique()

    # Fetch playlist items for each playlist
    playlist_items = []
    for playlist_id in unique_playlist_ids:
        try:
            print(f"🔄 Fetching items for playlist ID {playlist_id}...")
            result = sp.playlist_items(playlist_id)
            playlist_items.append(result)

        except Exception as e:
            print(f"⚠️ Error fetching items for playlist ID {playlist_id}: {e}")

    # Flatten JSON response
    df_playlist_items = pd.json_normalize(playlist_items)

    # Convert lists/dictionaries to strings
    for column in df_playlist_items.columns:
        if df_playlist_items[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df_playlist_items[column] = df_playlist_items[column].apply(str)

    # Write to SQL
    write_to_sql(df_playlist_items, "playlist_items", if_exists="replace")
    print("🎉 Playlist items fetched successfully!")

def delete_non_required_tables():
    """
        Delete non-required data from the local database.
    """

    delete_from_sql("user_tracks_history")

def check_database_connection():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(DATABASE_URL)
        print("🚀 Database connection successful!")
    except Exception as e:
        print(f"⚠️ Database connection error: {e}")
        exit()

# 🔹 Run the ETL process
if __name__ == "__main__":
    print("Job running at:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # check the database connection
    check_database_connection()

    # Run the ETL process
    fetch_user_tracks_history()
    format_user_tracks_history()
    fetch_album_data_for_user_tracks()
    fetch_track_data_for_user_tracks()
    fetch_artist_data_for_user_tracks()
    fetch_user_followed_artists()
    fetch_user_playlists()
    fetch_artist_top_tracks()
    fetch_user_saved_albums()
    get_new_releases_albums()
    fetch_playlist_items()
    fetch_artist_related_artists() # no data available

    # delete non-required tables
    delete_non_required_tables()

    print("🎉 ETL process completed successfully!")
    print("Job finished at:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
