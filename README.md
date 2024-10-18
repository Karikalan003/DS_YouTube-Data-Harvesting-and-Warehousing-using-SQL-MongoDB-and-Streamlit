# DS_YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit
DS_YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

LinkedIn : https://www.linkedin.com/in/karikalan-r-3210b1

# Overview
This project is designed to harvest YouTube data through its API and store the collected data in a SQL database. A Streamlit web application is used to display and analyze this data. The project allows users to retrieve various YouTube metrics (videos, channels, playlists) and provides a platform for easy interaction with the data. This solution is suitable for anyone looking to analyze YouTube data for insights or archival purposes.

Features
YouTube Data Harvesting: Fetch video, channel, and playlist details using the YouTube Data API.
SQL Database: Store and manage harvested data in a structured SQL database for efficient querying.
Streamlit Dashboard: Visualize and interact with the data using a clean, easy-to-use Streamlit interface.
Data Analysis: Perform data analysis and extract insights, including video performance, engagement, and growth trends.

# Requirements

Python 
Packages Installation
API integration
Data Collection
Streamlit
MongoDB
Postgre SQL
YouTube Data API Key
Pandas


Create a Postgre SQL database to store the YouTube data. 

Get YouTube API Key:

Sign up for a YouTube Data API key by following this guide.

Set up environment variables:

# Created user defined functions:
Below functions are to create the data frame for the several information from the youtube API
1.get_channel_info, 
2.get_video_ids, 
3.get_video_info, 
4.get_comment_info, 
5.get_playlist_info

# Insert collected data to MongoDB and Stored:
Through using mongo connection have stored all the data frames are stored in MongoDB.



Data Stored in SQL
Channel: 
Channel Data: Channel ID, Title, Subscriber Count, Video Count, and Description.
Video Data: Video ID, Title, View Count, Like Count, Comment Count, and Video Duration.
Playlist Data: Playlist ID, Title, Video Count, and associated channel.
Streamlit Dashboard
The Streamlit dashboard provides a visual interface to:

Display harvested data
Search channels or videos by keywords
Analyze video statistics like views, likes, comments, and more
Plot trends in subscriber growth, video popularity, etc.
Contributing
If you want to contribute to this project, feel free to create a pull request or raise an issue. Contributions are always welcome!

License
This project is licensed under the MIT License.
