from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from pymongo import MongoClient
from streamlit_option_menu import option_menu
import plotly.express as px

#Getting API Connection
def api_connection():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = 'AIzaSyBUrmJFiJjCt-eBTV1_HtnBqAaCyTnjh58'
    
 
    youtube = build(api_service_name,api_version,developerKey =api_key)
    
    return youtube 

youtube = api_connection()


#getting channel information 

   
def get_channel_info(channel_id): #creating user defined fn
    request =  youtube.channels().list(                         
                                        part = "snippet,contentDetails,statistics", 
                                        id = channel_id 
    )
    response = request.execute() #Executing the Request
        

    for item in response['items']:   
        data =dict(
                        channel_name = item["snippet"]["title"],
                        channel_id = item["id"],
                        subscription_count = item["statistics"]["subscriberCount"],
                        channel_views = item["statistics"]["viewCount"],
                        Total_Videos=item["statistics"]["videoCount"],
                        channel_description = item["snippet"]["description"],
                        playlist_id = item["contentDetails"]["relatedPlaylists"]["uploads"]
        )

    return data 


def get_video_ids(channel_id): #The function get_video_ids retrieves all video IDs from a YouTube channel's uploads playlist
    video_ids = [] 
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute() 
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token = None #Initialize Pagination Control Variable
    
    while True:
        response1 = youtube.playlistItems().list(  
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])   #Extract Video IDs
        next_page_token = response1.get('nextPageToken') #Update Pagination Token
        
        if next_page_token is None:
            break           #Break Loop if No More Pages
    
    return video_ids  


#Getting Video Information

def get_video_info(video_ids):
    
    video_info=[]

    for video_id in video_ids:
        request=youtube.videos().list(part="snippet,contentDetails,statistics",
                                        id=video_id)

        response=request.execute()

        for item in response["items"]:
            data=dict(  Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Video_Name=item['snippet']['title'],
                        Video_Description=item['snippet'].get('description'),
                        Tags=item['snippet'].get('tags'),
                        Published_Date=item['snippet']['publishedAt'],
                        View_Count=item['statistics'].get('viewCount'), 
                        Like_Count=item['statistics'].get('likeCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Duration=item['contentDetails']['duration'],
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Caption_Status=item['contentDetails']['caption'],
                        Definition=item['contentDetails']['definition'],
                        Comments_Count=item['statistics'].get('commentCount')
            )
                        
            video_info.append(data)    
    return video_info


#Getting Comment Information

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50 
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                
                Comment_data.append(data)
                
    except: 
        pass
    return Comment_data

#no changes required here 

#Getting Playlist Details

def get_playlist_info(channel_id):
        next_page_token=None
        Playlist_data =[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50, #max results can be 50 
                        pageToken=next_page_token #beyond 50 result token will automatically fetch next page
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Playlist_Name=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                Published_At=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount']
                        )
                        Playlist_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return Playlist_data

# Connect to MongoDB and upload data
client = MongoClient("mongodb://localhost:27017/")
db = client["youtube_data_harvesting"]  # Creating database 

def insert_into_mongo(channel_id):
    # Collect all data
    channel_details = get_channel_info(channel_id)
    playlist_details = get_playlist_info(channel_id)
    video_id = get_video_ids(channel_id)
    video_details = get_video_info(video_id)
    comment_details = get_comment_info(video_id)
    

    
    collection1 = db["insert_into_mongo"]  # Define collection
    collection1.insert_one({
        "get_channel_info": channel_details,
        "get_playlist_info": playlist_details,
        "get_video_info": video_details,
        "get_comment_info": comment_details
    })

    return "Upload completed successfully"


    
    #Table creation for for Channel,Playlist, Videos ,Comment



def channel_table(Channel_Name_Single):
   
    # Establish a connection to the local PostgreSQL database
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",        # Changed from 'username' to 'user'
        password="hari",
        database="youtube_data_harvesting",
        port="5432"
    )
    cursor = mydb.cursor()
  
    # SQL query to create the table if it does not already exist
 

    create_query = '''CREATE TABLE IF NOT EXISTS channel   (channel_name VARCHAR (100),
                                                            channel_id VARCHAR (100) PRIMARY KEY,
                                                            subscription_count BIGINT,
                                                            channel_views BIGINT,
                                                            total_videos INT,
                                                            channel_description TEXT,
                                                            playlist_id VARCHAR (100)
    )'''
    
    cursor.execute(create_query)
    mydb.commit()


    print("Channels table already created")



# Call the function to create the table

    Single_Channel_List = []

    # Connect to the database
    client = MongoClient("mongodb://localhost:27017/")
    db = client["youtube_data_harvesting"]

    # Select the collection
    collection1 = db["insert_into_mongo"]

    for channel_data in collection1.find({"get_channel_info.channel_name":Channel_Name_Single},{"_id": 0}):
        Single_Channel_List.append(channel_data["get_channel_info"])

    df_single_channel_list  = pd.DataFrame(Single_Channel_List)

    cursor = mydb.cursor()

    for index,row in df_single_channel_list.iterrows(): 
        insert_query = '''INSERT INTO channel  (channel_name,
                                                channel_id,
                                                subscription_count,
                                                channel_views,
                                                Total_Videos,
                                                channel_description,
                                                playlist_id
        )
                                                
                                                VALUES(%s,%s,%s,%s,%s,%s,%s)'''
                                                
                                                
        values = (row["channel_name"],
                    row["channel_id"],
                    row["subscription_count"],
                    row["channel_views"],
                    row["Total_Videos"],
                    row["channel_description"],
                    row["playlist_id"]
        )
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            text = f"Given {Channel_Name_Single} channel is already exists"

            return text




       
        

#table creation for getting playlist
def playlist_table(Channel_Name_Single):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",       
        password="hari",
        database="youtube_data_harvesting",
        port="5432"
    )

    cursor = mydb.cursor()

    # SQL query to create the table if it does not already exist
   
    create_query = '''CREATE TABLE IF NOT EXISTS playlist  (Playlist_Name VARCHAR (100),
                                                            Playlist_Id VARCHAR (100) PRIMARY KEY,
                                                            Channel_Name VARCHAR (100),
                                                            Channel_Id VARCHAR (100),
                                                            Published_At TIMESTAMP,
                                                            Video_Count VARCHAR (80)
    )'''
    
    cursor.execute(create_query)
    mydb.commit()


# Instantiate the MongoDB client
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017/")

    Single_Playlist_List = []

    # Connect to the database
    db = client["youtube_data_harvesting"]

    # Select the collection
    collection1 = db["insert_into_mongo"]

    for channel_data in collection1.find({"get_channel_info.channel_name":Channel_Name_Single},{"_id": 0}):
        Single_Playlist_List.append(channel_data["get_playlist_info"])

    df_single_playlist_list = pd.DataFrame(Single_Playlist_List[0])


    for index,row in df_single_playlist_list.iterrows(): 
        insert_query = '''INSERT INTO playlist (Playlist_Name,
                                                Playlist_Id,
                                                Channel_Name,
                                                Channel_Id,
                                                Published_At,
                                                Video_Count
        )
                                                    
                                            VALUES(%s,%s,%s,%s,%s,%s)'''
                                                
                                                
        values = (  row["Playlist_Name"],
                    row["Playlist_Id"],
                    row["Channel_Name"],
                    row["Channel_Id"],
                    row["Published_At"],
                    row["Video_Count"]
        )

    


        cursor.execute(insert_query,values)
        mydb.commit()




def video_table(Channel_Name_Single):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",        # Changed from 'username' to 'user'
        password="hari",
        database="youtube_data_harvesting",
        port="5432"
    )

    cursor = mydb.cursor()

    # SQL query to create the table if it does not already exist

    create_query = '''CREATE TABLE IF NOT EXISTS video (Channel_Name VARCHAR (100),
                                                        Channel_Id VARCHAR (100),
                                                        Video_Id VARCHAR (50) PRIMARY KEY,
                                                        Video_Name VARCHAR (200),
                                                        Video_Description TEXT,
                                                        Tags TEXT,
                                                        Published_Date TIMESTAMP, 
                                                        View_Count BIGINT,
                                                        Like_Count BIGINT,
                                                        Favorite_Count INT,
                                                        Duration INTERVAL,
                                                        Thumbnail VARCHAR (200),
                                                        Caption_Status VARCHAR (50),
                                                        Definition VARCHAR (50),
                                                        Comments_Count INT
    )'''


    cursor.execute(create_query)
    mydb.commit()


    # Instantiate the MongoDB client
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017/")

    Single_Video_List = []

    # Connect to the database
    db = client["youtube_data_harvesting"]

    # Select the collection
    collection1 = db["insert_into_mongo"]

    for channel_data in collection1.find({"get_channel_info.channel_name":Channel_Name_Single},{"_id": 0}):
        Single_Video_List.append(channel_data["get_video_info"])

    df_single_Video_list = pd.DataFrame(Single_Video_List[0])


    for index,row in df_single_Video_list.iterrows(): 
        insert_query = '''INSERT INTO video    (Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Video_Name,
                                                Video_Description,
                                                Tags,
                                                Published_Date,
                                                View_Count,
                                                Like_Count,
                                                Favorite_Count,
                                                Duration,
                                                Thumbnail,
                                                Caption_Status,
                                                Definition,
                                                Comments_Count
        )
                                                
                                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                        
                                                
        values = (  row["Channel_Name"],
                    row["Channel_Id"],
                    row["Video_Id"],
                    row["Video_Name"],
                    row["Video_Description"],
                    row["Tags"],
                    row["Published_Date"],
                    row["View_Count"],
                    row["Like_Count"],
                    row["Favorite_Count"],
                    row["Duration"],
                    row["Thumbnail"],
                    row["Caption_Status"],
                    row["Definition"],
                    row["Comments_Count"]
        )
    
        cursor.execute(insert_query,values)
        mydb.commit()



def comment_table(Channel_Name_Single):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",       
        password="hari",
        database="youtube_data_harvesting",
        port="5432")

    cursor = mydb.cursor()

    # SQL query to create the table if it does not already exist

    create_query = '''CREATE TABLE IF NOT EXISTS comment   (
                                                            Comment_Id VARCHAR (100) PRIMARY KEY,
                                                            Video_Id VARCHAR (100),
                                                            Comment_Author VARCHAR (200),
                                                            Comment_Text TEXT,
                                                            Comment_Published TIMESTAMP
    )'''




    cursor.execute(create_query)
    mydb.commit()

    # Instantiate the MongoDB client
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017/")

    Single_Comment_List = []

    # Connect to the database
    db = client["youtube_data_harvesting"]

    # Select the collection
    collection1 = db["insert_into_mongo"]

    for channel_data in collection1.find({"get_channel_info.channel_name":Channel_Name_Single},{"_id": 0}):
        Single_Comment_List.append(channel_data["get_comment_info"])

    df_single_Comment_list = pd.DataFrame(Single_Comment_List[0])

    for index,row in df_single_Comment_list.iterrows(): 
            insert_query = '''INSERT INTO comment(  
                                                    Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published
            )
                                                    
                                                    VALUES(%s,%s,%s,%s,%s)'''
                                            


            values = (  
                        row["Comment_Id"],
                        row["Video_Id"],
                        row["Comment_Text"],
                        row["Comment_Author"],
                        row["Comment_Published"]
            )


            cursor.execute(insert_query,values)
            mydb.commit()



def tables(Single_Channel):

    text = channel_table(Single_Channel)
    if text:
        st.write(text)
    else:
        playlist_table(Single_Channel)
        video_table(Single_Channel)
        comment_table(Single_Channel)

        return "Tables are created successfully"


def show_channel_table(table_name):

    channel_list = []
    client = MongoClient("mongodb://localhost:27017/")
    db = client["youtube_data_harvesting"]

    collection1 = db["insert_into_mongo"]

    for channel_data in collection1.find({}, {"_id": 0, "get_channel_info": 1}):
        channel_list.append(channel_data["get_channel_info"])
    
    df_channel = st.dataframe(channel_list)
    
    return df_channel


def show_playlist_table(table_name):

    playlist_list = []

    # Connect to the database
    client = MongoClient("mongodb://localhost:27017/")
    db = client["youtube_data_harvesting"]

    # Select the collection
    collection1 = db["insert_into_mongo"]

    # Fetch and print the channel data
    for playlist_data in collection1.find({}, {"_id": 0, "get_playlist_info": 1}):
        for i in range(len(playlist_data["get_playlist_info"])):
            playlist_list.append(playlist_data["get_playlist_info"][i])

    df_playlist = st.dataframe(playlist_list)

    return df_playlist
   


def show_video_table(table_name):

    video_list = []

    # Connect to the database
    client = MongoClient("mongodb://localhost:27017/")
    db = client["youtube_data_harvesting"]

    # Select the collection
    collection1 = db["insert_into_mongo"]

    # Fetch and append the video data to the list
    for video_data in collection1.find({}, {"_id": 0, "get_video_info": 1}):
        for i in range(len(video_data["get_video_info"])):
            video_list.append(video_data["get_video_info"][i])

    df_video = st.dataframe(video_list)

    return df_video


def show_comment_table(table_name):

    comment_list = []

    # Connect to the database
    client = MongoClient("mongodb://localhost:27017/")
    db = client["youtube_data_harvesting"]

    # Select the collection
    collection1 = db["insert_into_mongo"]

    # Fetch and print the channel data
    for comment_data in collection1.find({}, {"_id": 0, "get_comment_info": 1}):
        for i in range(len(comment_data["get_comment_info"])):
            comment_list.append(comment_data["get_comment_info"][i])

    df_comment = st.dataframe(comment_list)

    return df_comment

# streamlit is a user interface 

#streamlit part



with st.sidebar: #creating sidebar on the webpage it is like "about" information about project
    st.image(r"C:\Users\dell\Desktop\GUVI Projects\Youtube Project\Images\Simple.png",width=100)
    st.title(":red[**INSIGHTS**]") #main title 
    st.markdown("<h1 style='color: black;'><u>Core Skills Learned</u></h1>", unsafe_allow_html=True)  
    st.subheader("***Python-Based Scripting***")
    st.subheader("***Data Acquisition***")
    st.subheader("***MongoDB***")
    st.subheader("***API Integration***")
    st.subheader("***Data Handling with MongoDB and SQL***")

st.image(r"C:\Users\dell\Desktop\GUVI Projects\Youtube Project\Images\logo2.png")
st.title("DATA HARVESTING AND WAREHOUSING")
channel_id = st.text_input("Specify the Channel ID") #inputing tab


if st.button(":green[**Collect and Store Data in MongoDB**]"):
    channel_ids = []
    
    client = MongoClient("mongodb://localhost:27017/")
    db = client["youtube_data_harvesting"] 
    collection1 = db["insert_into_mongo"]
    for channel_data in collection1.find({}, {"_id": 0, "get_channel_info": 1}):
        channel_ids.append(channel_data["get_channel_info"]["channel_id"])

    if channel_id in channel_ids: #given channel id is already there in DB print some message and not perform any operation
        st.success("Channel Details of the given channel id is already exists")

    else:
        insert = insert_into_mongo(channel_id) #here it is performing some operations
        st.success(insert)
        
        

All_Channels = []

client = MongoClient("mongodb://localhost:27017/")
db = client["youtube_data_harvesting"]

collection1 = db["insert_into_mongo"]

for channel_data in collection1.find({}, {"_id": 0, "get_channel_info": 1}):
    All_Channels.append(channel_data["get_channel_info"]["channel_name"])

unique_channel = st.selectbox("Select the channel",All_Channels) # expression to only select specific channel to upload into SQL

if st.button(":blue[**Migrate to SQL**]"):
    Tables = tables(unique_channel) 
    st.success(Tables) #which means it shows sucess message in green colour

show_table = st.radio("CHOOSE THE TABLE TO DISPLAY",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS")) 

if show_table == "CHANNELS":

    st.subheader("TABLE VIEW")
    show_channel_table("channel")


elif show_table == "PLAYLISTS":

    st.subheader("TABLE VIEW")
    show_playlist_table("playlist")

elif show_table == "VIDEOS":

    st.subheader("TABLE VIEW")
    show_video_table("video")

elif show_table == "COMMENTS":

    st.subheader("TABLE VIEW")
    show_comment_table("comment")

#SQL Connection

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",       
    password="hari",
    database="youtube_data_harvesting",
    port="5432"
)

cursor = mydb.cursor()

Question = st.selectbox("Select Your Question",("1.What are the names of all the videos and their corresponding channels",
                                                 "2.Which channels have the most number of videos, and how many videos do they have?",
                                                 "3.What are the top 10 most viewed videos and their respective channels?",
                                                 "4.How many comments were made on each video, and what are their corresponding video names?",
                                                 "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                 "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                                 "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                 "8.What are the names of all the channels that have published videos in the year2022?",
                                                 "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                 "10.Which videos have the highest number of comments, and what are their corresponding channel names?")
) #"selectbox" choosing questions


mydb = psycopg2.connect(
    host="localhost",
    user="postgres",        
    password="hari",
    database="youtube_data_harvesting",
    port="5432"
)

cursor = mydb.cursor()

if Question == "1.What are the names of all the videos and their corresponding channels":

    query_1 = '''SELECT channel_name, COUNT(video_name) AS video_title_count 
                FROM video
                GROUP BY channel_name
                ORDER BY video_title_count DESC;'''
    
    cursor.execute(query_1)
    table_1 = cursor.fetchall()
    mydb.commit()

    DF_1 = pd.DataFrame(table_1,columns=["channel_name","video_title_count"])

    fig_bar_1 = px.bar(data_frame= DF_1, x = "channel_name", y = "video_title_count",
                                                    title= f"TOP CHANNEL VIDEO TITLE COUNT",
                                                    width=600 , height=650,hover_name="channel_name",
                                                    color_discrete_sequence= px.colors.sequential.Greens_r)
                
    st.plotly_chart(fig_bar_1)

    st.subheader("TABLE VIEW")
    query1 = '''SELECT video_name AS video_title , channel_name AS channelname FROM video'''
    cursor.execute(query1)
    mydb.commit()
    table1 = cursor.fetchall()
    df_table1 = pd.DataFrame(table1,columns=["video title","channel name"])

    #col1,col2 = st.columns() we can add here
    st.dataframe(df_table1,use_container_width=True)

elif Question == "2.Which channels have the most number of videos, and how many videos do they have?":

    query2 = '''SELECT channel_name AS channelname, Total_Videos AS Number_of_Videos FROM channel
                ORDER BY Total_Videos DESC'''

    cursor.execute(query2)
    mydb.commit()
    table2 = cursor.fetchall()
    df_table2 = pd.DataFrame(table2, columns=["Channel Name", "No of Videos"])

    fig_bar_2 = px.bar(data_frame= df_table2, x = "No of Videos", y = "Channel Name",orientation="h",
                                                    title= f"VIDEO COUNTS",
                                                    width=600 , height=650, hover_name= "Channel Name",
                                                    color_discrete_sequence= px.colors.sequential.Reds_r)
                
    st.plotly_chart(fig_bar_2)
    st.subheader("TABLE VIEW")
    st.write(df_table2)


    
elif Question == "3.What are the top 10 most viewed videos and their respective channels?":

    query3 = '''SELECT view_count AS view_count, channel_name AS channelname,video_name AS video_title from video
                WHERE view_count is NOT NULL ORDER BY view_count DESC LIMIT 10'''

    cursor.execute(query3)
    mydb.commit()
    table3 = cursor.fetchall()
    df_table3 = pd.DataFrame(table3, columns=["Views", "Channel Name","Video_Title"])

    fig_funnel_3 = px.funnel_area (data_frame= df_table3, names = "Channel Name", values = "Views",
                                                    title= f"MOST VIEWED VIDEOS",
                                                    width=600 , height=650, 
                                                    color_discrete_sequence= px.colors.sequential.Blues_r)
                
    st.plotly_chart(fig_funnel_3)
    st.subheader("TABLE VIEW")
    st.write(df_table3)

elif Question == "4.How many comments were made on each video, and what are their corresponding video names?":


    query_4 = '''SELECT channel_name, SUM(comments_count) AS Comment_Count 
                    from video
                    GROUP BY channel_name
                    ORDER BY Comment_Count DESC;'''
    
    cursor.execute(query_4)
    mydb.commit()
    table_4 = cursor.fetchall()
    DF_3 = pd.DataFrame(table_4, columns=["channel_name", "Comment_Count"])

    fig_pie_4 = px.pie(data_frame= DF_3, names = "channel_name", values = "Comment_Count",
                                                    title= f"TOP COMMENTS COUNT",
                                                    width=600 , height=650,hover_name="channel_name",
                                                    color_discrete_sequence= px.colors.sequential.Plasma)
            
    st.plotly_chart(fig_pie_4)
    


    query4 = '''SELECT video_name AS video_title , comments_count AS Comment_Count from video
                WHERE comments_count is NOT NULL ORDER BY comments_count DESC'''

    cursor.execute(query4)
    mydb.commit()
    table4 = cursor.fetchall()
    df_table4 = pd.DataFrame(table4, columns=["Video Title", "Comment Count"])
    st.subheader("TABLE VIEW")
    st.write(df_table4)

elif Question == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":

    query5 = '''SELECT channel_name AS channel_name ,video_name AS video_name , like_count AS likes from video
                WHERE like_count is NOT NULL ORDER BY like_count DESC LIMIT 10'''
    

    cursor.execute(query5)
    mydb.commit()
    table5 = cursor.fetchall()
    df_table5 = pd.DataFrame(table5, columns=["channel_name", "video_title" , "likes"])

    fig_bar_5 = px.bar(data_frame=df_table5, x="channel_name", y="likes",  # Use 'likes' in lowercase
                   title="TOP COMMENTS COUNT", width=1000, height=650,
                   hover_data=["channel_name", "video_title", "likes"],hover_name= "channel_name", # Ensure 'likes' is lowercase
                   color_discrete_sequence=px.colors.qualitative.Dark24_r)
            
    st.plotly_chart(fig_bar_5)
    st.subheader("TABLE VIEW")
    st.write(df_table5)

    

elif Question == "6.What is the total number of likes for each video, and what are their corresponding video names?":

    query6 = '''SELECT channel_name, video_name AS video_title, like_count AS Likes 
             FROM video
             WHERE like_count IS NOT NULL 
             ORDER BY like_count DESC'''
    
    cursor.execute(query6)
    mydb.commit()
    table6 = cursor.fetchall()
    df_table6 = pd.DataFrame(table6, columns=["channel_name", "video_title", "Likes"])
    
    query_6 = '''SELECT channel_name, SUM(like_count) AS Likes 
                    from video
                    GROUP BY channel_name
                    ORDER BY Likes DESC;'''
    
    cursor.execute(query_6)
    mydb.commit()
    table_6 = cursor.fetchall()
    df_table6 = pd.DataFrame(table_6, columns=["channel_name", "Likes"])

      # Use consistent case

    # Create the line chart
    fig_line_6 = px.line(data_frame=df_table6, 
                        x="channel_name", 
                        y="Likes",  # Use 'Likes' in the correct case
                        title="TOP LIKES COUNT", 
                        width=1000, 
                        height=650,
                        hover_data=["channel_name", "Likes"],  # Ensure consistent case
                        color_discrete_sequence=px.colors.qualitative.Plotly)

    # Display the chart in Streamlit
    st.plotly_chart(fig_line_6)

    st.subheader("TABLE VIEW")
    st.write(df_table6)

elif Question == "7.What is the total number of views for each channel, and what are their corresponding channel names?":

    query7 = '''SELECT channel_name AS channelname ,video_name AS video_title , view_count AS Views from video
                WHERE view_count is NOT NULL ORDER BY view_count DESC'''

    cursor.execute(query7)
    mydb.commit()
    table7 = cursor.fetchall()
    df_table7 = pd.DataFrame(table7, columns=["Channel Name","Video Title" , "Views"])


    query_7 = '''SELECT channel_name , SUM(view_count) AS Views 
                    from video
                    GROUP BY channel_name
                    ORDER BY Views DESC;'''
    
    cursor.execute(query_7)
    mydb.commit()
    table_7 = cursor.fetchall()
    df_table7 = pd.DataFrame(table_7, columns=["channel_name", "Views"])

      # Use consistent case

    # Create the line chart
    fig_line_7 = px.line(data_frame=df_table7, 
                        x="channel_name", 
                        y="Views",  # Use 'Likes' in the correct case
                        title="TOP VIEWS COUNT", 
                        width=1000, 
                        height=650,
                        hover_data=["channel_name", "Views"],  # Ensure consistent case
                        color_discrete_sequence=px.colors.qualitative.Plotly)
    st.plotly_chart(fig_line_7)

    st.subheader("TABLE VIEW")
    st.write(df_table7)

elif Question == "8.What are the names of all the channels that have published videos in the year2022?":

    query8 = '''SELECT channel_name AS channelname , video_name AS video_title ,published_date AS published from video
                WHERE EXTRACT(YEAR FROM published_date)=2022'''

    cursor.execute(query8)
    mydb.commit()
    table8 = cursor.fetchall()
    df_table8 = pd.DataFrame(table8, columns=["Channel Name","Video Title" , "Published Date"])

    query_8 = '''SELECT channel_name, COUNT(published_date) AS published_2022
                FROM video
                WHERE EXTRACT(YEAR FROM published_date) = 2022
                GROUP BY channel_name;'''
    
    cursor.execute(query_8)
    mydb.commit()
    table_8 = cursor.fetchall()
    df_table8 = pd.DataFrame(table_8, columns=["channel_name", "published_2022"])

    fig_bar_8 = px.bar(data_frame=df_table8, x="channel_name", y="published_2022",  # Use 'likes' in lowercase
                   title="TOP COMMENTS COUNT", width=1000, height=650,
                   hover_data=["channel_name" ,"published_2022"],hover_name= "channel_name", # Ensure 'likes' is lowercase
                   color_discrete_sequence=px.colors.qualitative.Set2)

    st.plotly_chart(fig_bar_8)

    st.subheader("TABLE VIEW")
    st.write(df_table8)

elif Question == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":

    query9 = '''SELECT channel_name AS channelname , AVG (duration) AS Average_Duration from video GROUP BY channel_name'''

    cursor.execute(query9)
    mydb.commit()
    table9 = cursor.fetchall()
    df_table9 = pd.DataFrame(table9, columns=["Channel_Name","Average_Duration"])

    Table9 = []

    for index,row in df_table9.iterrows():
        Channel_Title = row["Channel_Name"] 
        Average_Duration = row["Average_Duration"]
        Average_Duration_STR = str(Average_Duration)
        Table9.append(dict(Channeltitle = Channel_Title, AVG_duration = Average_Duration_STR))

    df_table9_str = pd.DataFrame(Table9)

    fig_line_9 = px.line(data_frame=df_table9, 
                        x="Channel_Name", 
                        y="Average_Duration",  # Use 'Likes' in the correct case
                        title="AVERAGE VIDEO DURATION", 
                        width=1000, 
                        height=650,
                        hover_data=["Channel_Name", "Average_Duration"],  # Ensure consistent case
                        color_discrete_sequence=px.colors.qualitative.Plotly)
    
    st.plotly_chart(fig_line_9)
    
    st.subheader("TABLE VIEW")
    st.write(df_table9_str)

elif Question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":

    query10 = '''SELECT channel_name AS channelname , video_name AS video_title, comments_count AS Comment_Count from video 
                WHERE comments_count IS NOT NULL ORDER BY comments_count DESC LIMIT 10;'''

    cursor.execute(query10)
    mydb.commit()
    table10 = cursor.fetchall()
    df_table10 = pd.DataFrame(table10, columns=["Channel_Name","Video Title","Comment Count"])

    fig_bar_10 = px.bar(data_frame=df_table10, x="Channel_Name", y="Comment Count",  # Use 'likes' in lowercase
                   title="TOP COMMENTS COUNT", width=1000, height=650,
                   hover_data=['Channel_Name',"Comment Count"],hover_name= "Video Title", # Ensure 'likes' is lowercase
                   color_discrete_sequence=px.colors.qualitative.Dark24_r)

    st.plotly_chart(fig_bar_10)
    st.subheader("TABLE VIEW")
    st.write(df_table10)


#----------------------------------------------------------------End----------------------------------------------------------------------------
