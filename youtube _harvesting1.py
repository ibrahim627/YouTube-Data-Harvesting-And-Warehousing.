import streamlit as st
import googleapiclient.discovery
from pymongo import MongoClient
import mysql.connector

# Set up MongoDB connection
mongo_client = MongoClient("<mongodb_connection_string>")
mongo_db = mongo_client["youtube_data_lake"]
mongo_collection = mongo_db["videos"]

# Set up MySQL connection
mysql_connection = mysql.connector.connect(
    host="<mysql_host>",
    user="<mysql_user>",
    password="<mysql_password>",
    database="<mysql_database>"
)
mysql_cursor = mysql_connection.cursor()

# Initialize YouTube API client
youtube_api = googleapiclient.discovery.build("youtube", "v3", developerKey="<your_api_key>")

# Streamlit app
def main():
    st.title("YouTube Data Harvesting and Warehousing")

    channel_id = st.text_input("Enter YouTube Channel ID")

    if st.button("Retrieve Channel Data"):
        channel_data = get_channel_data(channel_id)
        st.write("Channel Name:", channel_data["channel_name"])
        st.write("Subscribers:", channel_data["subscribers"])
        st.write("Total Video Count:", channel_data["video_count"])
        st.write("Playlist ID:", channel_data["playlist_id"])

        if st.button("Store Data in MongoDB"):
            store_data_in_mongodb(channel_data)

    if st.button("Migrate Data to SQL"):
        migrate_data_to_sql()

    st.write("Search and Retrieve Data from SQL:")
    # Provide search options and SQL query inputs here

# Function to retrieve channel data from YouTube API
def get_channel_data(channel_id):
    request = youtube_api.channels().list(
        part="snippet,statistics",
        id=channel_id
    )
    response = request.execute()

    # Extract relevant data from API response
    channel_data = {
        "channel_name": response["items"][0]["snippet"]["title"],
        "subscribers": response["items"][0]["statistics"]["subscriberCount"],
        "video_count": response["items"][0]["statistics"]["videoCount"],
        "playlist_id": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    }

    return channel_data

# Function to store channel data in MongoDB
def store_data_in_mongodb(channel_data):
    mongo_collection.insert_one(channel_data)

# Function to migrate data from MongoDB to SQL
def migrate_data_to_sql():
    # Retrieve data from MongoDB
    cursor = mongo_collection.find({})
    data = list(cursor)

    # Create SQL table and insert data
    mysql_cursor.execute("CREATE TABLE IF NOT EXISTS youtube_videos (channel_name VARCHAR(255), subscribers INT, video_count INT, playlist_id VARCHAR(255))")

    for video in data:
        query = "INSERT INTO youtube_videos (channel_name, subscribers, video_count, playlist_id) VALUES (%s, %s, %s, %s)"
        values = (video["channel_name"], video["subscribers"], video["video_count"], video["playlist_id"])
        mysql_cursor.execute(query, values)

    mysql_connection.commit()

# Function to search and retrieve data from SQL
def search_data_from_sql():
    # Write SQL queries based on search options and execute them using mysql_cursor

    # Retrieve and display the results using st.write()

# Run the Streamlit app
if __name__ == "__main__":
    main()
