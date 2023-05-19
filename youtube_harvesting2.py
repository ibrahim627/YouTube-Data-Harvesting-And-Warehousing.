import googleapiclient.discovery
from pymongo import MongoClient

# Set up MongoDB connection
mongo_client = MongoClient("<mongodb_connection_string>")
mongo_db = mongo_client["youtube_data"]
mongo_collection = mongo_db["videos"]

# Initialize YouTube API client
youtube_api = googleapiclient.discovery.build("youtube", "v3", developerKey="<your_api_key>")

# Function to extract and store data in MongoDB
def extract_and_store_data(channel_id):
    # Retrieve channel data
    channel_data = get_channel_data(channel_id)

    # Store channel data in MongoDB
    mongo_collection.insert_one(channel_data)

    # Retrieve playlist items
    playlist_items = get_playlist_items(channel_data["Playlist_Id"])

    # Store video data in MongoDB
    for item in playlist_items:
        video_data = get_video_data(item["contentDetails"]["videoId"])
        mongo_collection.insert_one(video_data)

# Function to retrieve channel data from YouTube API
def get_channel_data(channel_id):
    request = youtube_api.channels().list(
        part="snippet,statistics",
        id=channel_id
    )
    response = request.execute()

    # Extract relevant channel data
    channel_data = {
        "Channel_Name": {
            "Channel_Name": response["items"][0]["snippet"]["title"],
            "Channel_Id": response["items"][0]["id"],
            "Subscription_Count": response["items"][0]["statistics"]["subscriberCount"],
            "Channel_Views": response["items"][0]["statistics"]["viewCount"],
            "Channel_Description": response["items"][0]["snippet"]["description"],
            "Playlist_Id": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        }
    }

    return channel_data

# Function to retrieve playlist items from YouTube API
def get_playlist_items(playlist_id):
    request = youtube_api.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    # Extract playlist items
    playlist_items = response["items"]

    return playlist_items

# Function to retrieve video data from YouTube API
def get_video_data(video_id):
    request = youtube_api.videos().list(
        part="snippet,statistics",
        id=video_id
    )
    response = request.execute()

    # Extract relevant video data
    video_data = {
        "Video_Id": {
            "Video_Id": response["items"][0]["id"],
            "Video_Name": response["items"][0]["snippet"]["title"],
            "Video_Description": response["items"][0]["snippet"]["description"],
            "Tags": response["items"][0]["snippet"]["tags"],
            "PublishedAt": response["items"][0]["snippet"]["publishedAt"],
            "View_Count": response["items"][0]["statistics"]["viewCount"],
            "Like_Count": response["items"][0]["statistics"]["likeCount"],
            "Dislike_Count": response["items"][0]["statistics"]["dislikeCount"],
            "Favorite_Count": response["items"][0]["statistics"]["favoriteCount"],
            "Comment_Count": response["items"][0]["statistics"]["commentCount"],
            "Duration": response["items"][0]["contentDetails"]["duration"],
            "Thumbnail": response["items"][0]["snippet"]["thumbnails"]["default"]["url"],
            "Caption_Status": response["items"][0]["snippet"]["localized"]["hasCaption"]
        }
    }

    # Retrieve and store comments
    comments = get_video_comments(video_id)
    video_data["Video_Id"]["Comments"] = comments

    return video
