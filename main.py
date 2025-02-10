import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
import json
import re

load_dotenv()

# 🔹 PostgreSQL Database Connection
DATABASE_URL = os.getenv("DATABASE_URL").strip()
schema_name = os.getenv("SCHEMA_NAME").strip()
engine = create_engine(DATABASE_URL)

# 🔹 Spotify API Credentials
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID").strip()
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET").strip()
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI").strip()

print("DATABASE_URL:", DATABASE_URL , " type:", type(DATABASE_URL))
print("SCHEMA_NAME:", schema_name , " type:", type(schema_name))
print("SPOTIFY_CLIENT_ID:", SPOTIFY_CLIENT_ID , " type:", type(SPOTIFY_CLIENT_ID))
print("SPOTIFY_CLIENT_SECRET:", SPOTIFY_CLIENT_SECRET , " type:", type(SPOTIFY_CLIENT_SECRET))
print("SPOTIFY_REDIRECT_URI:", SPOTIFY_REDIRECT_URI , " type:", type(SPOTIFY_REDIRECT_URI))

# 🔹 Spotify Authentication
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET,
    redirect_uri=SPOTIFY_REDIRECT_URI,
    scope='user-read-recently-played user-read-email user-read-private playlist-read-private playlist-read-collaborative user-follow-read user-top-read user-library-read'
))

# Helper function to execute a custom SQL query and return the result as a DataFrame
def __execute_sql_query(query):
    """
    Executes a custom SQL query and returns the result as a Pandas DataFrame.
    
    Parameters:
    - query (str): The SQL query to execute.
    
    Returns:
    - pd.DataFrame: A DataFrame containing the query results.
    """
    try:
        # Create a connection to the PostgreSQL database
        with engine.connect() as connection:
            # Execute the query and load the result into a DataFrame
            df = pd.read_sql_query(query, connection)
        return df
    except Exception as e:
        print(f"⚠️ Error executing SQL query: {e}")
        return None

# Function to Read Data from SQL
def __read_from_sql(table_name):
    """
    Read data from a PostgreSQL table and return as a pandas DataFrame.

    Parameters:
    - table_name (str): Name of the table in the database.
    - schema_name (str): The schema of the table in the database.

    Returns:
    - pd.DataFrame: The query result.
    """
    try:
        print(f"📖 Reading data from table '{schema_name}.{table_name}'...")
        query = f"SELECT * FROM {schema_name}.{table_name};"
        dataframe = pd.read_sql(query, con=engine)
        return dataframe
    except Exception as e:
        print(f"Error reading from SQL: {e}")
        return None

# Function to Write Data to SQL
def __write_to_sql(dataframe, table_name, if_exists='append'):
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
        dataframe = __flatten_dataframe(dataframe)

        # Inspect table structure
        inspector = inspect(engine)
        table_full_name = f"{schema_name}.{table_name}"

        try:
            # Check if table exists
            existing_tables = inspector.get_table_names(schema=schema_name)
            if table_name in existing_tables:
                print(f"✅ Table '{table_full_name}' exists. Checking for new columns...")

                # Get existing column names
                existing_columns = {col["name"] for col in inspector.get_columns(table_name, schema=schema_name)}

                # Identify new columns
                new_columns = [col for col in dataframe.columns if col not in existing_columns]
                if new_columns:
                    print(f"➕ Adding new columns: {new_columns}")
                    with engine.connect() as connection:
                        transaction = connection.begin()  # Start transaction
                        try:
                            for column in new_columns:
                                alter_query = text(f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN "{column}" TEXT NULL')
                                connection.execute(alter_query)
                                print(f"✅ Successfully added column '{column}' to '{table_full_name}'")

                            transaction.commit()  # Commit changes before inserting data
                            print("🔄 Committed column additions successfully.")
                        except Exception as col_error:
                            transaction.rollback()  # Rollback on failure
                            print(f"⚠️ Error adding column '{column}': {col_error}")

            else:
                print(f"🚀 Table '{table_full_name}' does not exist. It will be created.")

        except Exception as inspect_error:
            print(f"⚠️ Error inspecting table '{table_full_name}': {inspect_error}")

        # Debug: Print the DataFrame to ensure it's correct
        print("📊 DataFrame content before inserting:")
        print(dataframe.head())
        print(dataframe.shape)

        # Write to SQL
        try:
            dataframe.to_sql(table_name, con=engine, if_exists=if_exists, index=False, schema=schema_name)
            print(f"✅ Data successfully written to '{table_full_name}'")
        except Exception as write_error:
            print(f"⚠️ Error writing DataFrame to '{table_full_name}': {write_error}")

    except Exception as e:
        print(f"⚠️ General Error in __write_to_sql function: {e}")

# Function to Delete Data from SQL
def __delete_from_sql(table_name, sqlQuery=None):
    """
    Delete data from a SQL database, with schema consideration.

    Parameters:
    - table_name: str, the name of the SQL table.
    - sqlQuery: str, custom SQL query to run (optional).
    """
    try:
        print(f"␡  Deleting data from table '{schema_name}.{table_name}'...")

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
                print(f"❎ Data successfully deleted from table '{schema_name}.{table_name}'")
            except Exception as e:
                transaction.rollback()  # Roll back if something goes wrong
                print(f"An exception occurred: {str(e)}")

    except Exception as e:
        print(f"An exception occurred: {str(e)}")
        return None

def __flatten_dataframe(df):
    """
    Recursively converts lists and dictionaries in DataFrame columns to strings
    until no such values remain.
    """
    while any(df[col].apply(lambda x: isinstance(x, (list, dict))).any() for col in df.columns):
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(str)
    return df

def extract_spotify_data():
    """
    Extract recently played tracks from Spotify API (last 24 hours).
    Handles pagination to fetch all available tracks.
    """
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    yesterday_unix = int(yesterday.timestamp() * 1000)

    print("🔄 Fetching recently played songs from Spotify...")

    results = sp.current_user_recently_played(limit=10, after=yesterday_unix)
    all_tracks = results.get("items", [])  # Initialize with the first page's tracks

    # Loop through pages of results
    while results.get("next"):
        print("➡️  Fetching next page of current_user_recently_played data...")
        results = sp.next(results)  # Get the next page of results
        new_tracks = results.get("items", [])
        all_tracks.extend(new_tracks)

    df = pd.json_normalize(all_tracks)  # Flatten the JSON response into a DataFrame

    if df.empty:
        print("⚠️ No recent tracks found!")
        return pd.DataFrame()  # Return an empty DataFrame if no data

    # Rename columns with periods to underscores
    df.columns = df.columns.str.replace('.', '_')
    
    return df

def fetch_user_tracks_history():

    df_spotify = extract_spotify_data()
    if df_spotify.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # print(df_spotify.columns)

    # Convert the JSON string to a Python object (list of dictionaries)
    # df_spotify["track_album_artists"] = df_spotify["track_album_artists"].apply(safe_json_loads)

    # Extract 'name' and 'id' safely
    # df_spotify["album_artists_name"] = df_spotify["track_album_artists"].apply(
    #     lambda x: x[0]["name"] if isinstance(x, list) and x and isinstance(x[0], dict) else None
    # )

    # df_spotify["album_artists_id"] = df_spotify["track_album_artists"].apply(
    #     lambda x: x[0]["id"] if isinstance(x, list) and x and isinstance(x[0], dict) else None
    # )

    df_spotify["track_album_image"] = df_spotify["track_album_images"].apply(
        lambda x: x[0]["url"] if isinstance(x, list) and x and isinstance(x[0], dict) else None
    )

    if df_spotify.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    __write_to_sql(df_spotify, "user_tracks_history")
    # df_spotify.to_csv("spotify_tracks.csv", index=False, encoding="utf-8")
    print("🎉 User Track History extracted successfully!")

def format_user_tracks_history():
    # Read data from SQL
    df = __read_from_sql("user_tracks_history")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Convert played_at to datetime
    df["played_at"] = pd.to_datetime(df["played_at"])

    # select only necessary columns
    df = df[["played_at", "track_album_album_type", "track_album_external_urls_spotify", "track_album_id", "track_album_name", "track_album_release_date", "track_duration_ms", "track_id", "track_name", "track_popularity", "track_external_urls_spotify", "context_external_urls_spotify", "context_type", "track_album_artists", "track_album_images", "track_album_image"]]

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

    table_name = "user_tracks_history_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 User Track History formatted successfully!")

    # add indexes
    create_indexes(table_name, ["track_id", "album_id"])
    print("🔍 Indexes created successfully!")

def fetch_album_data_for_user_tracks():
    # Read specific data from SQL
    query = f"""
    SELECT DISTINCT album_id
    FROM {schema_name}.user_tracks_history_formatted
    WHERE album_id IS NOT NULL;
    """

    df = __execute_sql_query(query)

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique album IDs
    unique_album_ids = df["album_id"].unique()

    # remove None values
    unique_album_ids = [x for x in unique_album_ids if x is not None]

    # Fetch album data from Spotify API with pagination
    album_data = []
    for album_id in unique_album_ids:
        try:
            print(f"🔄 Fetching album data for ID {album_id}...")
            result = sp.album(album_id)
            album_data.append(result)

            # Check if there are more pages of album data (if applicable)
            while result.get("next"):
                print("➡️  Fetching next page of album data...")
                result = sp.next(result)
                album_data.append(result)

        except Exception as e:
            print(f"⚠️ Error fetching album data for ID {album_id}: {e}")

    # Flatten JSON response
    df_albums = pd.json_normalize(album_data)

    if df_albums.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_albums.columns = df_albums.columns.str.replace('.', '_')

    # print(df_albums.columns)

    # Write to SQL
    __write_to_sql(df_albums, "album_data")
    print("🎉 Album data fetched successfully!")

def fetch_track_data_for_user_tracks():
    # Read specific data from SQL
    query = f"""
    SELECT DISTINCT track_id
    FROM {schema_name}.user_tracks_history_formatted
    WHERE track_id IS NOT NULL;
    """

    df = __execute_sql_query(query)
    
    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique track IDs
    unique_track_ids = df["track_id"].unique()

    # remove None values
    unique_track_ids = [x for x in unique_track_ids if x is not None]

    # Fetch track data from Spotify API with pagination
    track_data = []
    for track_id in unique_track_ids:
        try:
            print(f"🔄 Fetching track data for ID {track_id}...")
            result = sp.track(track_id)
            track_data.append(result)

            # Check if there are more pages of track data (if applicable)
            while result.get("next"):
                print("➡️  Fetching next page of track data...")
                result = sp.next(result)
                track_data.append(result)

        except Exception as e:
            print(f"⚠️ Error fetching track data for ID {track_id}: {e}")

    # Flatten JSON response
    df_tracks = pd.json_normalize(track_data)

    if df_tracks.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_tracks.columns = df_tracks.columns.str.replace('.', '_')

    # print(df_tracks.columns)

    # Convert the JSON string to a Python object (list of dictionaries)
    # df_tracks["artists"] = df_tracks["artists"].apply(safe_json_loads)

    # Extract the 'name' and 'id' fields
    # df_tracks["artists_name"] = df_tracks["artists"].apply(lambda x: x[0]["name"] if isinstance(x, list) and isinstance(x[0], dict) else None)
    # df_tracks["artists_id"] = df_tracks["artists"].apply(lambda x: x[0]["id"] if isinstance(x, list) and isinstance(x[0], dict) else None)

    # Write to SQL
    __write_to_sql(df_tracks, "track_data")
    print("🎉 Track data fetched successfully!")

def fetch_artist_data_for_user_tracks():
    # Read specific data from SQL
    query = f"""
    SELECT track_album_artists
    FROM {schema_name}.user_tracks_history_formatted
    WHERE track_album_artists IS NOT NULL;
    """

    df = __execute_sql_query(query)

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Initialize a set to store unique artist IDs
    unique_artist_ids = set()

    # Iterate over the dataframe and extract artist IDs
    for _, row in df.iterrows():
        try:
            artist_list = json.loads(row["track_album_artists"].replace("'", '"'))  # Handle single quotes
            for artist in artist_list:
                unique_artist_ids.add(artist["id"])
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"Error processing row: {row['track_album_artists']} - {e}")

    # Convert set to list
    unique_artist_ids = list(unique_artist_ids)

    # remove None values
    unique_artist_ids = [x for x in unique_artist_ids if x is not None]

    # Fetch artist data from Spotify API with pagination
    artist_data = []
    for artist_id in unique_artist_ids:
        try:
            print(f"🔄 Fetching artist data for ID {artist_id}...")
            result = sp.artist(artist_id)
            artist_data.append(result)

            # Check if there are more pages of artist data (if applicable)
            while result.get("next"):
                print("➡️  Fetching next page of artist data...")
                result = sp.next(result)
                artist_data.append(result)

        except Exception as e:
            print(f"⚠️ Error fetching artist data for ID {artist_id}: {e}")

    # Flatten JSON response
    df_artists = pd.json_normalize(artist_data)

    if df_artists.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_artists.columns = df_artists.columns.str.replace('.', '_')

    # print(df_artists.columns)

    # Write to SQL
    __write_to_sql(df_artists, "artist_data")
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
            print("➡️  Fetching next page of followed artists data...")
            results = sp.next(results["artists"])
        else:
            results = None

    # Flatten JSON response
    df_followed_artists = pd.json_normalize(followed_artists)

    if df_followed_artists.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_followed_artists.columns = df_followed_artists.columns.str.replace('.', '_')

    # print(df_followed_artists.columns)

    # Write to SQL
    __write_to_sql(df_followed_artists, "user_followed_artists")
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
            print("➡️  Fetching next page of user playlists data...")
            results = sp.next(results)
        else:
            results = None

    # Flatten JSON response
    df_user_playlists = pd.json_normalize(user_playlists)

    if df_user_playlists.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_user_playlists.columns = df_user_playlists.columns.str.replace('.', '_')

    # print(df_user_playlists.columns)

    # Write to SQL
    __write_to_sql(df_user_playlists, "user_playlists")
    print("🎉 User playlists fetched successfully!")

def fetch_artist_top_tracks():
    # Read specific data from SQL
    query = f"""
    SELECT DISTINCT id
    FROM {schema_name}.artists_formatted
    WHERE id IS NOT NULL;
    """

    df = __execute_sql_query(query)

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique artist IDs
    unique_artist_ids = df["id"].unique()

    # remove None values
    unique_artist_ids = [x for x in unique_artist_ids if x is not None]

    # Fetch top tracks for each artist
    top_tracks = []
    for artist_id in unique_artist_ids:
        try:
            print(f"🔄 Fetching top tracks for artist ID {artist_id}...")
            result = sp.artist_top_tracks(artist_id)

            for track in result.get("tracks", []):
                track_info = {
                    "artist_id": artist_id,
                    "track_id": track["id"],
                    **track
                }
                top_tracks.append(track_info)

        except Exception as e:
            print(f"⚠️ Error fetching top tracks for artist ID {artist_id}: {e}")

    # Flatten JSON response
    df_top_tracks = pd.json_normalize(top_tracks)

    if df_top_tracks.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_top_tracks.columns = df_top_tracks.columns.str.replace('.', '_')

    # print(df_top_tracks.columns)

    # Write to SQL
    __write_to_sql(df_top_tracks, "artist_top_tracks")
    print("🎉 Top tracks fetched successfully!")

def fetch_artist_related_artists():
    # Read data from SQL
    df = __read_from_sql("artist_data")
    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique artist IDs
    unique_artist_ids = df["id"]
    
    # remove None values
    unique_artist_ids = [x for x in unique_artist_ids if x is not None]

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

    if df_related_artists.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_related_artists.columns = df_related_artists.columns.str.replace('.', '_')

    # Write to SQL
    __write_to_sql(df_related_artists, "artist_related_artists")
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
            print("➡️  Fetching next page of saved albums data...")
            results = sp.next(results)
        else:
            results = None

    # Flatten JSON response
    df_saved_albums = pd.json_normalize(saved_albums)

    if df_saved_albums.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_saved_albums.columns = df_saved_albums.columns.str.replace('.', '_')

    # print(df_saved_albums.columns)

    # Write to SQL
    __write_to_sql(df_saved_albums, "user_saved_albums")
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
            print("➡️  Fetching next page of new releases data...")
            results = sp.next(results["albums"])
        else:
            results = None

    # Flatten JSON response
    df_new_releases = pd.json_normalize(new_releases)

    if df_new_releases.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_new_releases.columns = df_new_releases.columns.str.replace('.', '_')

    # print(df_new_releases.columns)

    # Write to SQL
    __write_to_sql(df_new_releases, "new_releases_albums")
    print("🎉 New releases fetched successfully!")

def fetch_playlist_items():
    # Read specific data from SQL
    query = f"""
    SELECT DISTINCT id
    FROM {schema_name}.user_playlists_formatted
    WHERE id IS NOT NULL;
    """

    df = __execute_sql_query(query)

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Get unique playlist IDs
    unique_playlist_ids = df["id"].unique()

    # remove None values
    unique_playlist_ids = [x for x in unique_playlist_ids if x is not None]

    # Fetch playlist items for each playlist
    playlist_items = []
    for playlist_id in unique_playlist_ids:
        try:
            print(f"🔄 Fetching items for playlist ID {playlist_id}...")
            result = sp.playlist_items(playlist_id)

            # if result["items"] is empty, skip
            if not result["items"]:
                continue

            for item in result["items"]:
                track_info = {
                    "playlist_id": playlist_id,
                    **item
                }
                playlist_items.append(track_info)

        except Exception as e:
            print(f"⚠️ Error fetching items for playlist ID {playlist_id}: {e}")

    # Flatten JSON response
    df_playlist_items = pd.json_normalize(playlist_items)

    if df_playlist_items.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    df_playlist_items.columns = df_playlist_items.columns.str.replace('.', '_')

    # print(df_playlist_items.columns)

    # Write to SQL
    __write_to_sql(df_playlist_items, "playlist_items")
    print("🎉 Playlist items fetched successfully!")

def delete_non_required_tables():
    """
        Delete non-required data from the local database.
    """

    __delete_from_sql("user_tracks_history")
    __delete_from_sql("album_data")
    __delete_from_sql("track_data")
    __delete_from_sql("artist_data")
    __delete_from_sql("user_followed_artists")
    __delete_from_sql("user_playlists")
    __delete_from_sql("artist_top_tracks")
    __delete_from_sql("user_saved_albums")
    __delete_from_sql("new_releases_albums")
    __delete_from_sql("playlist_items")

    print("🗑️  Non-required tables deleted successfully!")

def check_database_connection():
    try:
        # Test connection
        with engine.connect() as connection:
            print("🚀 Database connection successful!")
            print("Host information: ", connection.engine.url)

    except Exception as e:
        print(f"⚠️ Database connection error: {e}")
        exit()

def format_album_data():
    # Read data from SQL
    df = __read_from_sql("album_data")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # select only necessary columns
    df = df[["album_type", "total_tracks", "id", "name", "release_date", "artists", "label", "popularity"]]

    # rename columns
    # df = df.rename(columns={
    #     "tracks.total": "tracks_total",
    # })

    table_name = "albums_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 Album Data formatted successfully!")

    # add indexes
    create_indexes(table_name, ["id"])
    print("🔍 Indexes created successfully!")

def format_track_data():
    # Read data from SQL
    df = __read_from_sql("track_data")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # select only necessary columns
    df = df[["duration_ms", "id", "name", "popularity", "track_number", "album_album_type", "album_id", "album_name", "album_release_date", "album_total_tracks", "artists"]]

    # rename columns
    df = df.rename(columns={
        "album_album_type": "album_type",
    })

    table_name = "tracks_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 Track Data formatted successfully!")

    # add indexes
    create_indexes(table_name, ["id", "album_id"])
    print("🔍 Indexes created successfully!")

def format_artist_data():
    # Read data from SQL
    df = __read_from_sql("artist_data")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # select only necessary columns
    df = df[["genres", "id", "name", "popularity", "external_urls_spotify", "followers_total"]]

    # rename columns
    df = df.rename(columns={
        "external_urls_spotify": "url",
        "followers_total": "followers",
    })

    table_name = "artists_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 Artist Data formatted successfully!")

    # add indexes
    create_indexes(table_name, ["id"])

def format_user_followed_artists():
    # Read data from SQL
    df = __read_from_sql("user_followed_artists")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # select only necessary columns
    df = df[["genres", "id", "name", "popularity", "external_urls_spotify", "followers_total"]]

    # rename columns
    df = df.rename(columns={
        "external_urls_spotify": "url",
        "followers_total": "followers",
    })

    table_name = "user_followed_artists_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 User Followed Artists formatted successfully!")

    # add indexes
    create_indexes(table_name, ["id"])
    print("🔍 Indexes created successfully!")

def format_user_playlists():
    # Read data from SQL
    df = __read_from_sql("user_playlists")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # select only necessary columns
    df = df[["id", "name", "public", "snapshot_id", "external_urls_spotify", "owner_display_name", "owner_id", "owner_external_urls_spotify", "owner_href", "tracks_href", "tracks_total"]]

    # rename columns
    df = df.rename(columns={
        "external_urls_spotify": "url",
        "owner_external_urls_spotify": "owner_url",
    })

    table_name = "user_playlists_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 User Playlists formatted successfully!")

    # add indexes
    create_indexes(table_name, ["id", "owner_id"])
    print("🔍 Indexes created successfully!")

def format_artist_top_tracks():
    # Read data from SQL
    df = __read_from_sql("artist_top_tracks")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Convert the 'tracks' column to a list of dictionaries
    df["tracks"] = df["tracks"].apply(lambda x: x["tracks"] if isinstance(x, dict) else None)

    # Extract the 'name' and 'id' fields
    df["track_name"] = df["tracks"].apply(lambda x: x[0]["name"] if isinstance(x, list) and isinstance(x[0], dict) else None)
    df["track_id"] = df["tracks"].apply(lambda x: x[0]["id"] if isinstance(x, list) and isinstance(x[0], dict) else None)

    # select only necessary columns
    df = df[["id", "name", "popularity", "track_name", "track_id"]]

    # rename columns
    df = df.rename(columns={
        "id": "artist_id",
        "name": "artist_name",
    })

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, "artist_top_tracks_formatted")
    print("🎉 Artist Top Tracks formatted successfully!")

def format_artist_top_tracks():
    # Read data from SQL
    df = __read_from_sql("artist_top_tracks")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # select only necessary columns
    df = df[["artist_id", "track_id", "duration_ms", "name", "popularity", "track_number", "album_album_type", "album_external_urls_spotify", "album_id", "album_name", "album_release_date", "album_total_tracks", "external_urls_spotify"]]

    # rename columns
    df = df.rename(columns={
        "album_album_type": "album_type",
        "album_external_urls_spotify": "album_url"
    })

    table_name = "artist_top_tracks_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 Artist Top Tracks formatted successfully!")

    # add indexes
    create_indexes(table_name, ["artist_id", "track_id", "album_id"])
    print("🔍 Indexes created successfully!")

def format_user_saved_albums():
    # Read data from SQL
    df = __read_from_sql("user_saved_albums")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Convert release_date to datetime
    df["added_at"] = pd.to_datetime(df["added_at"])

    # select only necessary columns
    df = df[["added_at", "album_total_tracks", "album_external_urls_spotify", "album_id", "album_name", "album_release_date", "album_tracks_limit", "album_tracks_total", "album_label", "album_popularity"]]

    # rename columns
    df = df.rename(columns={
        "album_external_urls_spotify": "album_url",
        "album_total_tracks": "total_tracks",
        "album_tracks_limit": "tracks_limit",
        "album_tracks_total": "tracks_total",
    })

    table_name = "user_saved_albums_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 User Saved Albums formatted successfully!")

    # add indexes
    create_indexes(table_name, ["album_id"])
    print("🔍 Indexes created successfully!")

def format_new_releases_albums():
    # Read data from SQL
    df = __read_from_sql("new_releases_albums")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # select only necessary columns
    df = df[["album_type", "artists", "id", "name", "release_date", "total_tracks", "external_urls_spotify"]]

    # rename columns
    df = df.rename(columns={
        "external_urls_spotify": "url",
    })

    table_name = "new_releases_albums_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 New Releases Albums formatted successfully!")

    # add indexes
    create_indexes(table_name, ["id"])
    print("🔍 Indexes created successfully!")

def format_playlist_items():
    # Read data from SQL
    df = __read_from_sql("playlist_items")

    if df is None:
        print("⚠️ No data found. Exiting ETL process.")
        return

    # Convert release_date to datetime
    df["added_at"] = pd.to_datetime(df["added_at"])

    # select only necessary columns
    df = df[["playlist_id", "added_at", "added_by_external_urls_spotify", "added_by_id", "track_album_id", "track_album_name", "track_album_release_date", "track_album_external_urls_spotify", "track_album_total_tracks", "track_track_number", "track_duration_ms", "track_external_urls_spotify", "track_id", "track_name", "track_popularity"]]

    # rename columns
    df = df.rename(columns={
        "added_by_external_urls_spotify": "added_by_url",
        "track_album_id": "album_id",
        "track_album_name": "album_name",
        "track_album_release_date": "album_release_date",
        "track_album_external_urls_spotify": "album_url",
        "track_album_total_tracks": "album_total_tracks",
        "track_external_urls_spotify": "track_url",
    })

    table_name = "playlist_items_formatted"

    if df.empty:
        print("⚠️ No data extracted. Exiting ETL process.")
        return

    # write to sql
    __write_to_sql(df, table_name)
    print("🎉 Playlist Items formatted successfully!")

    # add indexes
    create_indexes(table_name, ["playlist_id", "track_id", "album_id"])
    print("🔍 Indexes created successfully!")

def safe_json_loads(x):
    # If the input is already a list or dictionary, return it as-is
    if isinstance(x, (list, dict)):
        return x
    
    # If the input is a string, attempt to parse it as JSON
    if isinstance(x, str):
        try:
            x = x.replace("'", '"')
            return json.loads(x)
        except json.JSONDecodeError:
            print(f"❌ Skipping invalid JSON: {x}")  # Log invalid JSON for debugging
            return None  # Return None if JSON decoding fails
    
    # For all other types, return the input as-is
    return x

def create_indexes(table_name, columns):
    """
    Creates indexes on specified columns for a given table.

    Parameters:
    - table_name (str): The name of the target SQL table.
    - columns (list): List of column names to index.
    """
    try:
        with engine.connect() as connection:
            for col in columns:
                index_name = f"{table_name}_{col}_idx"
                print(f"🔍 Creating index '{index_name}' on '{table_name} ({col})'...")

                connection.execute(text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema_name}.{table_name} ({col});"))

        print(f"✅ Indexes successfully created for table '{schema_name}.{table_name}'.")
    except Exception as e:
        print(f"⚠️ Error creating indexes: {e}")

def main():
    print("Job running at:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # Check the database connection
    check_database_connection()

    # Fetch data from Spotify API
    fetch_user_tracks_history()
    format_user_tracks_history()

    fetch_album_data_for_user_tracks()
    format_album_data()

    fetch_track_data_for_user_tracks()
    format_track_data()

    fetch_artist_data_for_user_tracks()
    format_artist_data()

    fetch_user_followed_artists()
    format_user_followed_artists()

    fetch_user_playlists()
    format_user_playlists()

    fetch_artist_top_tracks()
    format_artist_top_tracks()

    fetch_user_saved_albums()
    format_user_saved_albums()

    get_new_releases_albums()
    format_new_releases_albums()

    fetch_playlist_items()
    format_playlist_items()

    # fetch_artist_related_artists() # no data available

    # Delete non-required tables
    delete_non_required_tables()

    print("🎉 ETL process completed successfully!")
    print("Job finished at:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# 🔹 Run the ETL process
if __name__ == "__main__":
    main()