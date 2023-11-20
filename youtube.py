#Importing the googleapiclient library and build module

import googleapiclient
from googleapiclient.discovery import build
from pprint import pprint
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi 
import mysql.connector as mysql
import pandas as pd 
import datetime
import isodate
import streamlit as st


#Creating a function to connect the api service and key throughout the program

def api_connection():
       api_service_name = "youtube"
       api_version = "V3"
       api_key = "AIzaSyBW5i8vq6m3lWELwntKFaOLHBCa9oQqN0w"

       youtube = build(api_service_name,api_version,developerKey=api_key)
       return youtube

#Creating a variable called youtube_call and asign the api_connection function to it 
#   By assigning that we can call the function whenever we want by  running that variable

youtube_call = api_connection()
#youtube_call

#Creatint a function called channel_details which contains channel id,name, description,playlist, views and video counts etc..

def get_channel_details(channel_id):
# Requesting the channel to provide CHANNEL DETAILS where it contains parts like id,snippet,startistics,content details

      request = youtube_call.channels().list(
           id = channel_id,
           part = "snippet,statistics,contentDetails"
          )
      response = request.execute()
 #Creating a for loop to access all the  requri
      for i in response["items"]:
        data = dict(
            channel_title = i["snippet"]["title"],
            channel_id= i["id"],
            channel_subscribers = i["statistics"]['subscriberCount'],
            channel_viewcount = i["statistics"]["viewCount"],
            channel_videocount = i["statistics"]["videoCount"],
            channel_description = i["snippet"]["description"],
            channel_playlistid = i["contentDetails"]["relatedPlaylists"]["uploads"]
            )
        
      return data 


#Creating a variable called channel_detail_call and assigning the channel_detail functions into that variable to call
 #whenever we want to use that function
 
#channel_detail_call =get_channel_details("UCykSoHEAMVSU_w6ACWlxlzA")



#Now requesting the youtube to provide PLAYLIST Id from playlists()

def get_video_ids(playlist_ids):
      playlist_request = youtube_call.channels().list(
      part = "contentDetails",
      id = playlist_ids
      )
      playlist_response = playlist_request.execute()
      playlist_id = playlist_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

      video_idlist = [] 

      #Creating a variable for pageToken and initilizing it with None
      next_page_token = None

      #This while loop runs until the pageToken becomes none and False , until that it get all the videos id form the playlist
      while True:
            #Now requesting the youtube to get video ids of the playlist from playlistItems()
            playlistItems_request = youtube_call.playlistItems().list(
                  part = "snippet,id",
                  playlistId = playlist_id,
                  maxResults = 50,
                  pageToken = next_page_token
            )
            playlistItem_response = playlistItems_request.execute()

            #To get all the videos id mention in the maxResults we are using for loop and appending that id's into the list
            for i in  range(len(playlistItem_response["items"])):
                  video_id = playlistItem_response["items"][i]["snippet"]["resourceId"]["videoId"]
                  video_idlist.append(video_id)
            next_page_token = playlistItem_response.get("nextPageToken")
            if next_page_token == None:
                  break
      return video_idlist      

#video_id_call = get_video_ids("UCnz-ZXXER4jOvuED5trXfEA")

#Now going to get video details like video name, likes , dislikes count and others
def get_video_details(video_detailslist_id):
    video_detailslist = []
    for i in video_detailslist_id:
        video_request = youtube_call.videos().list(
            part = "id,snippet,contentDetails,statistics",
            id = i 
        )
        video_response = video_request.execute()
        for i in video_response["items"]:
            video_data = {
                "channel_title" : i["snippet"]["channelTitle"],
                "channel_id" : i["snippet"]["channelId"],
                "video_title" : i["snippet"]["title"],
                "video_id" : i["id"],
                "video_thumnails" : i["snippet"]["thumbnails"]["default"]["url"],
                "video_publishDate" : i["snippet"]["publishedAt"],
                "video_duration" : i["contentDetails"]["duration"],
                "video_view" : i["statistics"].get("viewCount",),
                "video_likes" : i["statistics"].get("likeCount"),
                "video_favorite" : i["statistics"].get("favoriteCount"),
                "video_comments" : i["statistics"].get("commentCount"),
                "video_captionStatus" : i["contentDetails"]["caption"]

            }
        video_detailslist.append(video_data)
    return video_detailslist    
#Here video_d_call is the variable where we stored the video ids in a list in get_video_ids function
#video_details_call = get_video_details(video_id_call)

#Now we are going to get the comments
def get_comment_details(video_ids):
    comment_detaillist = []
    try:
        for i in video_ids:
                comment_request = youtube_call.commentThreads().list(
                    part = "snippet,id,replies",
                    videoId = i,
                    maxResults = 50
                )
                comment_response = comment_request.execute()

                for i in comment_response["items"]:
                    comment_data = {
                        "comment_id" : i["id"],
                        "video_id" : i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        "comment_text" : i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        "comment_author" :i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        "comment_publishdate" :i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    }
                    comment_detaillist.append(comment_data)
    except:
          pass
    return comment_detaillist    
#comment_detailslist_call = get_comment_details(video_id_call)


#Now to get the playlist detials 
def get_playlist_details(channel_ids):
    next_page_token = None
    playlist = []
    playlistdetails_request = youtube_call.playlists().list(
        part = "id,contentDetails,snippet",
        channelId = channel_ids,
        maxResults = 50,
        pageToken = next_page_token
    )
    playlistdetials_response = playlistdetails_request.execute()

    while True:
        for i in playlistdetials_response["items"]:
            playlist_data = {
                "playlist_id" : i["id"],
                "channel_id" : i["snippet"]["channelId"],
                "playlist_name" : i["snippet"]["title"]
            }
            playlist.append(playlist_data)
            
        next_page_token = playlistdetials_response.get("nextPageToken")    
        if next_page_token == None:
            break    
    return playlist    
            

#playlist_details_call = get_playlist_details("UCykSoHEAMVSU_w6ACWlxlzA")           

#MongoDB connection
client = pymongo.MongoClient("mongodb+srv://Nivetha:mongonivetha123@cluster0.adch71s.mongodb.net/?retryWrites=true&w=majority")
db = client["youtube_data"]


#Creating a function called channel details and assigning all the functions created to get 
#channel details,playlist details , video details and comment details'''

def channel_details(channel_id):
    channel_detail = get_channel_details(channel_id)
    playlist_details = get_playlist_details(channel_id)
    video_ids= get_video_ids(channel_id)
    video_details = get_video_details(video_ids)
    comment_details = get_comment_details(video_ids)

    col1 = db["channel_details"]
    col1.insert_one({"Channel_information" :channel_detail, "playlist_information" :playlist_details,
                      "Video_ids_information" : video_ids,"video_information" : video_details , "comment_information":comment_details })
    return  print("Insertion completed successfully")

#channel1_insertion = channel_details("UC6RTJC0JgdVn_CCJe23tf6w")   The Data Dude Youtube Channel    


    #Creating table for channel,playlist ,video, and comment details
def channels_table(): 
        mydb = mysql.connect(
            host = "localhost",
            user = "root",
            password = "mysql",
            database = "youtube_data",
            port = "3306"
        )

        cursor = mydb.cursor()

        drop_query = '''drop table if exists channels'''
        cursor.execute(drop_query)
        mydb.commit()
        
        #creating a table named channels
        try:
            channel_table ='''create table if not exists channels(channel_title varchar(100),channel_id varchar(70) primary key,
                            channel_subscribers bigint,channel_viewcount bigint, channel_videocount int,channel_description text)'''

            cursor.execute(channel_table)               
            mydb.commit()

        except:
            print("Channel table already created")  

        ch_list = []
        db = client["youtube_data"]   
        col1 = db["channel_details"]
        for i in col1.find({},{"_id":0,"Channel_information":1}):
            ch_list.append(i["Channel_information"])
        df = pd.DataFrame(ch_list)       
        
        #inserting the values of channels dataframe into the channels table
        for index,rows in df.iterrows():
            insert_query = '''insert into channels(channel_title,
                                                channel_id,
                                                channel_subscribers,
                                                channel_viewcount,
                                                channel_videocount,
                                                channel_description) 
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
            values = (rows["channel_title"],
                    rows["channel_id"],
                    rows["channel_subscribers"],
                    rows["channel_viewcount"],
                    rows["channel_videocount"],
                    rows["channel_description"])    

            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                print("Channel values already inserted")       


#Creating table for playlist details

def playlist_table():
    mydb = mysql.connect(
        host = "localhost", 
        user = "root",
        password = "mysql",
        database = "youtube_data",
        port = "3306"
    )

    cursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()
    
    #creating a table named playlists
    playlist_query ='''create table if not exists playlists(playlist_id varchar(100) primary key,channel_id varchar(70),
                                                                playlist_name text)'''

    cursor.execute(playlist_query)               
    mydb.commit()

    pl_list = []
    db = client["youtube_data"]   
    col1 = db["channel_details"]
    for i in col1.find({},{"_id":0,"playlist_information":1}):
        for j in range(len(i["playlist_information"])):
            pl_list.append(i["playlist_information"][j])
    df_playlist = pd.DataFrame(pl_list)  

    #inserting the values of playlist dataframe into the playlists table
    for index,rows in df_playlist.iterrows():
        insert_query = '''insert into playlists(playlist_id,channel_id,playlist_name)
                                            
                                            values(%s,%s,%s)'''
        values = (rows["playlist_id"],
                rows["channel_id"],
                rows["playlist_name"]
                )    

        cursor.execute(insert_query,values)
        mydb.commit()
    


#creating table for video details
def video_table():
    mydb = mysql.connect(
            host = "localhost", 
            user = "root",
            password = "mysql",
            database = "youtube_data",
            port = "3306"
        )

    cursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    #for creating a table named videos
    video_query ='''create table if not exists videos(channel_title varchar(50),channel_id varchar(70),video_title varchar(100),
                                        video_id varchar(50) primary key,video_thumnails varchar(200),
                                        video_publishDate timestamp,video_duration time,
                                        video_view bigint,video_likes bigint,video_favorite int,
                                        video_comments bigint,video_captionStatus varchar(20))'''

    cursor.execute(video_query)               
    mydb.commit()

    vd_list = []
    db = client["youtube_data"]   
    col1 = db["channel_details"]
    for i in col1.find({},{"_id":0,"video_information":1}):
        for j in range(len(i["video_information"])):
            vd_list.append(i["video_information"][j])
    df_video = pd.DataFrame(vd_list)  

    cursor = mydb.cursor()
    
    #inserting the values of video dataframe into videos table
    for index,rows in df_video.iterrows():
        insert_query = '''insert into videos(channel_title,channel_id,video_title,video_id,video_thumnails,video_publishDate,
                                video_duration,video_view,video_likes,video_favorite,video_comments,video_captionStatus)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        # Convert the string value to datetime object                                    
        dt = datetime.datetime.strptime(rows["video_publishDate"], "%Y-%m-%dT%H:%M:%SZ")
        # Format the datetime object to match MySQL timestamp format
        dt_formatted = dt.strftime("%Y-%m-%d %H:%M:%S")
        
        # Parse the string value to a timedelta object
        duration = isodate.parse_duration(rows["video_duration"])

        # Convert the timedelta object to a time object
        duration = (datetime.datetime.min + duration).time()

        values = (rows["channel_title"],
                rows["channel_id"],
                rows["video_title"],
                rows["video_id"],
                rows["video_thumnails"],
                dt_formatted,
                duration,
                rows["video_view"],
                rows["video_likes"],
                rows["video_favorite"],
                rows["video_comments"],
                rows["video_captionStatus"]
                )    

        cursor.execute(insert_query,values)
        mydb.commit()


#creating table for comment details
def comments_table():
    mydb = mysql.connect(
            host = "localhost", 
            user = "root",
            password = "mysql",
            database = "youtube_data",
            port = "3306"
        )

    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()
    #for creating a table named comments
    comment_query ='''create table if not exists comments(comment_id varchar(100) primary key,video_id varchar(70),comment_text text,
                                                            comment_author varchar(100),comment_publishdate timestamp)'''

    cursor.execute(comment_query)               
    mydb.commit()

    cmt_list = []
    db = client["youtube_data"]   
    col1 = db["channel_details"]
    for i in col1.find({},{"_id":0,"comment_information":1}):
        for j in range(len(i["comment_information"])):
            cmt_list.append(i["comment_information"][j])
    df_comment = pd.DataFrame(cmt_list)  

    #inserting the values of dataframe into the comments table
    for index,rows in df_comment.iterrows():
            insert_query = '''insert into comments(comment_id,video_id,comment_text,comment_author,comment_publishdate)
                                                
                                                values(%s,%s,%s,%s,%s)'''

            # Convert the string value to datetime object                                    
            dt = datetime.datetime.strptime(rows["comment_publishdate"], "%Y-%m-%dT%H:%M:%SZ")
            # Format the datetime object to match MySQL timestamp format
            dt_formatted = dt.strftime("%Y-%m-%d %H:%M:%S")

            values = (rows['comment_id'],
                    rows['video_id'],
                    rows['comment_text'],
                    rows['comment_author'],
                    dt_formatted
                    )    

            cursor.execute(insert_query,values)
            mydb.commit()



def tables():
    channels_table()
    playlist_table()
    video_table()
    comments_table()

    return "Table created successfully"
#tables_call =tables()

#creating a dataframe using streamlit
def show_channel_table():
    ch_list = []
    db = client["youtube_data"]   
    col1 = db["channel_details"]
    for i in col1.find({},{"_id":0,"Channel_information":1}):
        ch_list.append(i["Channel_information"])
    dfst_channel= st.dataframe(ch_list)  

    return dfst_channel


#creating streamlit dataframe for playlist table
def show_playlist_table():
    pl_list = []
    db = client["youtube_data"]   
    col1 = db["channel_details"]
    for i in col1.find({},{"_id":0,"playlist_information":1}):
        for j in range(len(i["playlist_information"])):
            pl_list.append(i["playlist_information"][j])
    dfst_playlist = st.dataframe(pl_list)  

    return dfst_playlist



#creating streamlit dataframe for videos table
def show_video_table():
    vd_list = []
    db = client["youtube_data"]   
    col1 = db["channel_details"]
    for i in col1.find({},{"_id":0,"video_information":1}):
        for j in range(len(i["video_information"])):
            vd_list.append(i["video_information"][j])
    dfst_video = st.dataframe(vd_list)  

    return dfst_video


#creating streamlit dataframe for comments table
def show_comment_table(): 
    cmt_list = []
    db = client["youtube_data"]   
    col1 = db["channel_details"]
    for i in col1.find({},{"_id":0,"comment_information":1}):
        for j in range(len(i["comment_information"])):
            cmt_list.append(i["comment_information"][j])
    dfst_comment = st.dataframe(cmt_list)  

    return dfst_comment



#Streamlit part
st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING USING SQL, MongoDB AND STREAMLIT]")
st.header("Hello!")

channel_id = st.text_input("Enter the Channel ID:")

if st.button("Collect and store the data"):
    ch_ids =[]
    db =client["youtube_data"]
    col1 = db["channel_details"]
    for i in col1.find({},{"_id":0,"Channel_information":1}):
        ch_ids.append(i["Channel_information"]["channel_id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel ID is already exists")      

    else:
        st.success("[IPB] Enterring befor channel details ") 
        insert = channel_details(channel_id)   
        st.success(insert) 

if st.button("Push data into MySql"):
    table_creation = tables()
    st.success(table_creation)

view_table = st.radio("SELECTED THE REQUIRED TABLE",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))  

if view_table == "CHANNELS":
    show_channel_table()
elif view_table == "PLAYLISTS":
    show_playlist_table()
elif view_table == "VIDEOS":
    show_video_table()
elif view_table == "COMMENTS":
    show_comment_table()    


#Question Part
#SQL connection
mydb = mysql.connect(
        host = "localhost", 
        user = "root",
        password = "mysql",
        database = "youtube_data",
        port = "3306"
    )

cursor = mydb.cursor()

questions = st.selectbox("Select you question here",("1.What are the names of all videos and its corresponding channels?",
          "2.Which channels have the most number of videos and its count?",
          "3.What are the top most viewed videos and their repective channels",
          "4.How many comments were on each video and what are their corresponding video names",
          "5.Which videos have highest likes and name its corresponding channel names?",
          "6.What is the total number of likes  for each video and what are their corresponding video names?",
          "7.What is the total number of views for each channel,and what are their channel name?",
          "8.What are the names of all the channels that have published videos in the year 2022",
          "9.What is the average duration of all videos in each channel, and what are their channel names?",
          "10.Which videos have the highest number of comments and what are their corresponding channel names?"))

#question 1
mydb = mysql.connect(
            host = "localhost",
            user = "root",
            password = "mysql",
            database = "youtube_data",
            port = "3306",
          #  consume_results = True
        )
cursor = mydb.cursor()
if questions=="1.What are the names of all videos and its corresponding channels?": 
    query1 = '''select video_title as video_name,channel_title as channel_name from videos'''
    cursor.execute(query1)
    t1 = cursor.fetchall()
    mydb.commit()
    df1 =pd.DataFrame(t1,columns=["video name","channel name"])
    st.write(df1)

#Question 2
elif questions=="2.Which channels have the most number of videos and its count?": 
  query2 = '''select channel_title as channel_name, channel_videocount as no_of_videos from channels order by channel_videocount desc'''
  cursor.execute(query2)
  t2 = cursor.fetchall()
  mydb.commit()
  df2 =pd.DataFrame(t2,columns=["channel name","Number of videos"])
  st.write(df2)

#Question 3
elif questions=="3.What are the top most viewed videos and their repective channels": 
  query3 = '''select channel_title as channel_name,video_title as video_name,video_view as view_count from videos 
                where video_view is not null order by video_view desc limit 10 '''
  cursor.execute(query3)
  t3 = cursor.fetchall()
  mydb.commit()
  df3 =pd.DataFrame(t3,columns=["channel name","video name","video views"])
  st.write(df3)      
  
#Question 4
elif questions=="3.What are the top most viewed videos and their repective channels": 
  query4 = '''select video_title as video_name,video_comments as comment_count from videos where video_comments is not null'''
  cursor.execute(query4)
  t4 = cursor.fetchall()
  mydb.commit()
  df4 =pd.DataFrame(t4,columns=["video name","Number of comments"])
  st.write(df4)      

#Question 5
elif questions=="5.Which videos have highest likes and name its corresponding channel names?": 
  query5 = '''select video_title as video_name,video_likes as likes_count from videos where video_likes is not null order by video_likes desc'''
  cursor.execute(query5)
  t5 = cursor.fetchall()
  mydb.commit()
  df5 =pd.DataFrame(t5,columns=["video name","likes count"])
  st.write(df5)  

#Question 6
elif questions=="6.What is the total number of likes  for each video and what are their corresponding video names?": 
  query6 = '''select video_title as video_name,video_likes as likes_count from videos'''
  cursor.execute(query6)
  t6 = cursor.fetchall()
  mydb.commit()
  df6 =pd.DataFrame(t6,columns=["video name","likes count"])
  st.write(df6)  

#Question 7
elif questions=="7.What is the total number of views for each channel,and what are their channel name?": 
  query7 = '''select channel_title as channel_name,channel_viewcount as channel_views from channels'''
  cursor.execute(query7)
  t7 = cursor.fetchall()
  mydb.commit()
  df7 =pd.DataFrame(t7,columns=["channel name","views count"])
  st.write(df7)  
 
#Question 8
elif questions=="8.What are the names of all the channels that have published videos in the year 2022": 
  query8 = '''select channel_title as channel_name,video_title as video_name,video_publishDate as video_releasedate from videos
                        where extract(year from video_publishDate)=2022'''
  cursor.execute(query8)
  t8 = cursor.fetchall()
  mydb.commit()
  df8 =pd.DataFrame(t8,columns=["channel name","video name","video published date"])
  st.write(df8)  

#Question 9
elif questions=="9.What is the average duration of all videos in each channel, and what are their channel names?": 
  query9 = '''select channel_title as channel_name,AVG(video_duration) as average_duration from videos group by channel_title'''
  cursor.execute(query9)
  t9 = cursor.fetchall()
  mydb.commit()
  df9 =pd.DataFrame(t9,columns=["channel name","average duration"])
  st.write(df9)

#Question 10
elif questions=="10.Which videos have the highest number of comments and what are their corresponding channel names?": 
  query10 = '''select channel_title as channel_name,video_title as video_name,video_comments as comments_count from videos
                        where video_comments is not null order by video_comments desc'''
  cursor.execute(query10)
  t10 = cursor.fetchall()
  mydb.commit()
  df10 =pd.DataFrame(t10,columns=["channel name","video name","Numbers of comments"])
  #df10
  st.write(df10)





