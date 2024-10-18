# DS_YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit
DS_YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

LinkedIn : https://www.linkedin.com/in/karikalan-r-3210b1

# Overview
This project is designed to harvest YouTube data through its API and store the collected data in a SQL database. A Streamlit web application is used to display and analyze this data. The project allows users to retrieve various YouTube metrics (videos, channels, playlists) and provides a platform for easy interaction with the data. This solution is suitable for anyone looking to analyze YouTube data for insights or archival purposes.

Features
YouTube Data Harvesting: Fetch video, channel, and playlist, comment details using the YouTube Data API.
MongoDB: Created data frames are stored in MongoDB database
SQL Database: Store and manage harvested data in a structured SQL database for efficient querying.
Streamlit Dashboard: Visualize and interact with the data using a clean, easy-to-use Streamlit interface.
Data Analysis: Perform data analysis and extract insights, including video performance, engagement, and growth trends and data viualization. 

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



# Created user defined functions:
Below functions are to create the data frames for getting several information from the youtube API
1.get_channel_info, 
2.get_video_ids, 
3.get_video_info, 
4.get_comment_info, 
5.get_playlist_info

# Insert collected data to MongoDB and Stored:
Through using mongo connection have stored all the data frames are stored in MongoDB.
naming all the data bases seperatley to access the DBs 


# Data Stored in SQL
Channel: 
1."channel" Data: 
   . channel_name, channel_idD, subscription_count, channel_views, total_videos, channel_description, playlist_id
   . Fetched the data columns from SQL by using Mongo and SQL connections through for loop
   . Function coded to load the data from data frame and mongoDB to SQL

2."playlist" Data:
   . Playlist_Name, Playlist_Id, Channel_Name, Channel_Id, Published_At, Video_Count
   . Fetched the data columns from SQL by using Mongo and SQL connections through for loop
   . Function coded to load the data from data frame and mongoDB to SQL

3."video" Data:
   . Channel_Name, Channel_Id, Video_Id, Video_Name, Video_Description, Tags, Published_Date, View_Count, 
     Like_Count, Favorite_Count, Duration, Thumbnail, Caption_Status, Definition, Comments_Count
   . Fetched the data columns from SQL by using Mongo and SQL connections through for loop
   . Function coded to load the data from data frame and mongoDB to SQL

4. "comment" Data:
   . Comment_Id, Video_Id, Comment_Author, Comment_Text, Comment_Published
   . Fetched the data columns from SQL by using Mongo and SQL connections through for loop
   . Function coded to load the data from data frame and mongoDB to SQL
   

   
# Streamlit Dashboard:
1. Created the sidebar and title interface 
2. "button" created to access the and load the data by using channel_id to MongoDB
3. "button" created to access the and migrate the data by using channel_id to Postgre SQL
4. "table" created for multiple data views such as channel, playlist, video, comment through "radio" button
5. write a code for creating data for given 10 questions
6. Data Visualization created for the above questions


Finally successfully created "streamlit" dashboard and accessed each options  





