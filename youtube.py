#import libraries

from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import json
import streamlit as st
from streamlit_option_menu import option_menu


# API CONNECTION 
def api_connect():
    api="AIzaSyCMW93iRBTwpl7pcXUqfq7RbFVkLiZ_JZU"

    api_service_name= 'youtube'

    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=api)
    
    return youtube

youtube=api_connect()
 
 #get_channels_information
def get_channel_info(channel_id):
    request = youtube.channels().list(
          part="snippet,ContentDetails,statistics",
          id=channel_id
          )
    response = request.execute()

    for i in response['items']:
         data=dict(channel_Name=i['snippet']['title'],
                   channel_id=i['id'],
                   subscribers=i['statistics']['subscriberCount'],
                   View_count=i['statistics']["viewCount"],
                   Total_Videos = i["statistics"]["videoCount"],
                   Channel_Description = i["snippet"]["description"],
                   upload_id = i["contentDetails"]["relatedPlaylists"]["uploads"],
                   )
    return data    

 #get playlist ids


def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data

# get Video_ids


def get_Video_ids(channel_id):
    Video_ids = []
    response=youtube.channels().list(
                        part='contentDetails',
                        id=channel_id).execute()
    upload_id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    nextPageToken=None

    while True:
        response1=youtube.playlistItems().list(
                            part='snippet',
                            playlistId=upload_id,
                            maxResults=50,
                            pageToken=nextPageToken).execute()

        for i in range(len(response1['items'])):
           Video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
           nextPageToken=response1.get('nextPageToken')
        if nextPageToken ==None:
            break
    return Video_ids

#get video information

def get_video_info(Video_ids):

    video_data = []

    for video_id in Video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


# FUNCTION TO GET COMMENT INFORMATION

def get_comment_info(Video_ids):
        Comment_Information = []
        try:
                for video_id in Video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 100
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information
    
 #pymongo connect
client=pymongo.MongoClient("mongodb+srv://gopiguru737:Gopi2708@cluster0.5dsjh2s.mongodb.net/")
db=client['YouTubeData']
coll1=db['channel_details']


#Function to upload all_data into Mongo Db 

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    play_details=get_playlist_info(channel_id)
    video_ids=get_Video_ids(channel_id)
    vi_details=get_video_info(video_ids)
    comment_details=get_comment_info(video_ids)
    db=client['YouTubeData']
    coll1=db['channel_details']
    coll1.insert_one({'Channel_Information':ch_details,'playlist_Information':play_details,'video_information':vi_details,'Comment_Information':comment_details})
    
    return "Upload Completed Sucessfully"

#Function to create channels table

def channels_table():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube_channels_Data' 

    )

    mycursor = mydb.cursor(buffered=True)

    drop_query='drop table if exists channels'
    mycursor.execute(drop_query)
    mydb.commit()


    try:
        query1="create table if not exists channels(channel_Name varchar(50),channel_id varchar(70) primary key,subscribers bigint,View_count bigint,Total_videos bigint,Channel_Description text,upload_id varchar(70))"
        mycursor.execute(query1)

        mydb.commit
        
    except:
        print('Channel already created')

    ch_List=[]
    db=client['YouTubeData']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'Channel_Information':1}):
        ch_List.append(ch_data['Channel_Information'])

    df=pd.DataFrame(ch_List)

    for index,row in df.iterrows():
        insert_query = '''INSERT into channels(channel_Name,
                                                    channel_id,
                                                    subscribers,
                                                    View_count,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    upload_id)
                                                VALUES(%s,%s,%s,%s,%s,%s,%s)'''
                

        values =(
                        row['channel_Name'],
                        row['channel_id'],
                        row['subscribers'],
                        row['View_count'],
                        row['Total_Videos'],
                        row['Channel_Description'],
                        row['upload_id'])
        try:
                mycursor.execute(insert_query,values)
                mydb.commit()
        except:
                st.write('channel already created')

#Function to create playlists Table

def playlist_table():

    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube_channels_Data'
    )

    mycursor = mydb.cursor(buffered=True)

    drop_query='drop table if exists Playlists'
    mycursor.execute(drop_query)
    mydb.commit()


    try:
        query1="create table if not exists Playlists (PlaylistId varchar(50) primary key,Title varchar(70),ChannelId varchar(100),ChannelName varchar(100),PublishedAt timestamp,VideoCount int)"
        mycursor.execute(query1)

        mydb.commit()
            
        
    except:
        st.write('Channel already created')

    pl_List=[]
    db=client['YouTubeData']
    coll1=db['channel_details']
    for ph_data in coll1.find({},{'_id':0,'playlist_Information':1}):
        for i in range(len(ph_data['playlist_Information'])):
                pl_List.append(ph_data['playlist_Information'][i])
            
    df1=pd.DataFrame(pl_List)


    for index,row in df1.iterrows():
            insert_query1 = '''INSERT into Playlists(PlaylistId,
                                                    Title,
                                                    ChannelId,
                                                    ChannelName,
                                                    PublishedAt,
                                                    VideoCount)
                                                VALUES(%s,%s,%s,%s,%s,%s)'''
        
            values =(
                        row['PlaylistId'],
                        row['Title'],
                        row['ChannelId'],
                        row['ChannelName'],
                        row['PublishedAt'],
                        row['VideoCount'])
            try:
                mycursor.execute(insert_query1,values)
                mydb.commit()    
                    
            except:
                st.write("videos values already inserted in the table")
 #Function to create videos Table                
                
def videos_table():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube_channels_Data'
)
    mycursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists videos(
                        Channel_Name varchar(150),
                        Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, 
                        Title varchar(150), 
                        Tags text,
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date timestamp,
                        Duration time, 
                        Views bigint, 
                        Likes bigint,
                        Comments int,
                        Favorite_Count int, 
                        Definition varchar(10), 
                        Caption_Status varchar(50) 
                        )''' 
                        
        mycursor.execute(create_query)             
        mydb.commit()
    except:
        st.write("Videos Table alrady created")

    vi_list = []
    db = client["YouTubeData"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
        
    
    for index, row in df2.iterrows():
        insert_query = '''
                    INSERT INTO videos (Channel_Name,
                        Channel_Id,
                        Video_Id, 
                        Title, 
                        Tags,
                        Thumbnail,
                        Description, 
                        Published_Date,
                        Duration, 
                        Views, 
                        Likes,
                        Comments,
                        Favorite_Count, 
                        Definition, 
                        Caption_Status 
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                '''
        values = (
                    row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    json.dumps(row['Tags']),
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])
                                
        try:    
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("videos values already inserted in the table")

#Function to create comments Table
                        
def comments_table():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube_channels_Data')
    
    
    mycursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)'''
        mycursor.execute(create_query)
        mydb.commit()
        
    except:
        st.write("Comments Table already created")

    com_list = []
    db = client["YouTubeData"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"Comment_Information":1}):
        for i in range(len(com_data["Comment_Information"])):
            com_list.append(com_data["Comment_Information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                      Video_Id ,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
            try:
                mycursor.execute(insert_query,values)
                mydb.commit()
            except:
                 st.write("This comments are already exist in comments table")

#Function to run all the tables

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    
    return "Tables Created successfully"

#Function to show channels table
   
def show_channels_table():
    ch_list = []
    db = client["YouTubeData"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"Channel_Information":1}):
        ch_list.append(ch_data["Channel_Information"])
    channels_table = st.dataframe(ch_list)
    
    return channels_table

#Function to Playlists table

def show_playlists_table():
    db = client["YouTubeData"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_Information":1}):
        for i in range(len(pl_data["playlist_Information"])):
                pl_list.append(pl_data["playlist_Information"][i])
    playlists_table = st.dataframe(pl_list)
    
    return playlists_table

#Function to show video Table

def show_videos_table():
    vi_list = []
    db = client["YouTubeData"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
               vi_list.append(vi_data["video_information"][i])
             
    videos_table = st.dataframe(vi_list)
    
    return videos_table

#Function to show_comments Table

def show_comments_table():
    com_list = []
    db = client["YouTubeData"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"Comment_Information":1}):
        for i in range(len(com_data["Comment_Information"])):
             com_list.append(com_data["Comment_Information"][i])
               
    comments_table = st.dataframe(com_list)
    
    return comments_table

with st.sidebar:
     selected=option_menu(menu_title='Menu',options=['Home','Extract Data and Upload into Mongo','Data Migration to SQl','Result'],
                          icons=['house-heart-fill','cloud-upload-fill','cloud-arrow-up-fill'],
                          menu_icon='list',

                          default_index=0
                          )
if selected=='Home':
     st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
     st.write('This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.')
     st.header("SKILL TAKE AWAY")
     st.caption('Python scripting')
     st.caption("Data Collection")
     st.caption("MongoDB")
     st.caption("API Integration")
     st.caption(" Data Managment using MongoDB and SQL")
        
if selected=='Extract Data and Upload into Mongo':
         
    channel_id = st.text_input("Enter the Channel id")
    channels = channel_id.split(',')
    channels = [ch.strip() for ch in channels if ch]

    if st.button("Collect and Store data"):
        for channel in channels:
            ch_ids = []
            db = client["YouTubeData"]
            coll1 = db["channel_details"]
            for ch_data in coll1.find({},{"_id":0,"Channel_Information":1}):
                        ch_ids.append(ch_data["Channel_Information"]["channel_id"])


            if channel in ch_ids:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            
            else:
                output = channel_details(channel)
                st.success(output)

    show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

    if show_table == ":green[channels]":
        show_channels_table()
    elif show_table == ":orange[playlists]":
        show_playlists_table()
    elif show_table ==":red[videos]":
        show_videos_table()
    elif show_table == ":blue[comments]":
        show_comments_table()

                

if selected=='Data Migration to SQl':
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube_channels_Data')

    mycursor = mydb.cursor()
        
    if st.button("Migrate to SQL"):
        display = tables()
        st.success(display)
    
if selected=='Result':

    #SQL connection
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube_channels_Data')

    mycursor = mydb.cursor()
        
    question = st.selectbox(
        'Please Select Your Question',
        ('1. All the videos and the Channel Name',
        '2. Channels with most number of videos',
        '3. 10 most viewed videos',
        '4. Comments in each video',
        '5. Videos with highest likes',
        '6. likes of all videos',
        '7. views of each channel',
        '8. videos published in the year 2022',
        '9. average duration of all videos in each channel',
        '10. videos with highest number of comments'))


    if question == '1. All the videos and the Channel Name':
        query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
        mycursor.execute(query1)
        t1=mycursor.fetchall()
        mydb.commit()
        st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

    elif question == '2. Channels with most number of videos':
        query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
        mycursor.execute(query2)
        t2=mycursor.fetchall()
        mydb.commit()
        st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

    elif question == '3. 10 most viewed videos':
        query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                            where Views is not null order by Views desc limit 10;'''
        mycursor.execute(query3)
        t3 = mycursor.fetchall()
        mydb.commit()
        st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

    elif question == '4. Comments in each video':
        query4 = "select Comments as No_of_comments ,Title as VideoTitle from videos where Comments is not null;"
        mycursor.execute(query4)
        t4=mycursor.fetchall()
        mydb.commit()
        st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

    elif question == '5. Videos with highest likes':
        query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                        where Likes is not null order by Likes desc;'''
        mycursor.execute(query5)
        t5 = mycursor.fetchall()
        mydb.commit()
        st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

    elif question == '6. likes of all videos':
        query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
        mycursor.execute(query6)
        t6 = mycursor.fetchall()
        mydb.commit()
        st.write(pd.DataFrame(t6, columns=["like count","video title"]))

    elif question == '7. views of each channel':
        query7 = "select Channel_Name as ChannelName, View_count as Channelviews from channels;"
        mycursor.execute(query7)
        t7=mycursor.fetchall()
        mydb.commit()
        st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

    elif question == '8. videos published in the year 2022':
        query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                    where extract(year from Published_Date) = 2022;'''
        mycursor.execute(query8)
        t8=mycursor.fetchall()
        mydb.commit()
        st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

    elif question == '9. average duration of all videos in each channel':
        query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
        mycursor.execute(query9)
        t9=mycursor.fetchall()
        mydb.commit()
        t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
        T9=[]
        for index, row in t9.iterrows():
            channel_title = row['ChannelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))

    elif question == '10. videos with highest number of comments':
        query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                        where Comments is not null order by Comments desc;'''
        mycursor.execute(query10)
        t10=mycursor.fetchall()
        mydb.commit()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
