from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import pymongo
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector
import pymongo
from isodate import parse_duration

with st.sidebar:
    selected = option_menu("Youtube Data Harvest", ["Landing Page", 'Upload data to MongoDB','SQL Data Transform', 'Queries'], 
        icons=['house', 'gear',"database"])
    st.subheader(':green[About]')
    st.markdown('YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit')
    st.subheader(':green[Domin]')
    st.markdown('Social Media')
if selected == 'Landing Page':
    st.subheader('The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.')
    st.markdown(':blue[1.Retrieve YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.]')
    st.markdown(':blue[2.Stored the data in a MongoDB database as a data lake.]')
    st.markdown(':blue[3.Collected data for up to 10 different YouTube channels and store them in the data lake by clicking a button]')
    st.markdown(':blue[4.Option to select a channel name and migrate its data from the data lake to a SQL database as tables.]')
    st.markdown(':blue[5.Search and retrieve data from the SQL database using different search options, including joining tables to get channel details.]')
api_key = 'AIzaSyByX7AgWea51Qe-zMMFittQWEC4Dc0Erzw'
youtube = build('youtube', 'v3', developerKey=api_key)
channel_id = ['UCwFkuGbV_C9ZoaEpOt9oSjQ',
             'UCraMH4wlj9hpkAz1DJAZfGw',
             'UC80pxHTMUWVYswuiikB8qgw',
             'UCwr-evhuzGZgDFrq_1pLt_A',
             'UCnYWQKgTNBMAuF84ANbI59Q',
             'UC5tiRM8t_h7N4rUU3DiVopQ',
             'UCHCFa2mrOXLKqCgB_338UYw',
             'UCZMT-px-ckBFrVGRB6Fjwsw',
             'UCz3-aVrGhqBytjDxruA0yWA',
             'UCpjB30EIZ2cDdPyZhSFzk2w'] 
def get_all_channels_data(channel_id):
    channel_data = []
    response = youtube.channels().list(
        part = 'snippet,contentDetails,statistics',
        id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        channel_data.append(data)
    return channel_data

def get_video_ids(channel_id):
    video_ids_all_channels = []
    for c_id in channel_id:
        response = youtube.channels().list(id=c_id, 
                                      part='contentDetails').execute()
        for i in range(len(response['items'])):
            playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = None

        while True:
            response = youtube.playlistItems().list(playlistId=playlist_id, 
                                               part='snippet', 
                                               maxResults=50,
                                               pageToken=next_page_token).execute()

            for i in range(len(response['items'])):
                video_ids_all_channels.append(response['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = response.get('nextPageToken')

            if next_page_token is None:
                break
    return video_ids_all_channels
video_ids = get_video_ids(channel_id)

def get_video_details(video_ids):
    video_status = []
    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                #Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = parse_duration(video['contentDetails']['duration']).total_seconds() / 60,
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_status.append(video_details)
    return video_status
def get_comment_details(video_ids):  
    all_comments = []
    try:
        for v_ids in range(len((video_ids))):
                request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_ids[v_ids],
                    maxResults = 50
                )
                response = request.execute()
                for cmt in response['items']:
                    all_comments.append({'Comment_id' : cmt['id'],
                                        'Video_id' : cmt['snippet']['videoId'],
                                        'Comment_text' : cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        'Comment_author' : cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        'Comment_posted_date' : cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                                        'Like_count' : cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                                        'Reply_count' : cmt['snippet']['totalReplyCount']
                                        })
                next_Page_token = response.get('nextPageToken')
                morePages = True
                while morePages:
                    if next_Page_token is None:
                        morePages = False
                    else:
                        request = youtube.commentThreads().list(
                            part="snippet,replies",
                            videoId= video_ids[v_ids],
                            maxResults = 50,
                            pageToken=next_Page_token
                            )
                        response = request.execute()
                        for i in range(len(response['items'])):
                            all_comments.append({'Comment_id' :cmt['id'],
                                                'Video_id' :cmt['snippet']['videoId'],
                                                'Comment_text' :cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                                                'Comment_author' :cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                                'Comment_posted_date' :cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                                                'Like_count' :cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                                                'Reply_count' :cmt['snippet']['totalReplyCount']
                                                })
    except:
        pass
    return all_comments

my_mongodb_connection = pymongo.MongoClient('mongodb://localhost:27017')
my_mongo_database = my_mongodb_connection['dw58_dw59_data_analysis']

def fetch_channel_name():
    channel_name = []
    for i in my_mongo_database['channel_details'].find():
        channel_name.append(i['Channel_name'])
    return channel_name

def comments():
    comment = []
    for i in get_video_ids(channel_id):
        comment += get_comment_details(i)
    return comment

my_sql_connection = mysql.connector.connect(host = 'localhost', user = 'root', password='1234')
mycursor = my_sql_connection.cursor()
#mycursor.execute('create database Guvi_Youtube_Project')
mycursor.execute('use Guvi_Youtube_Project')

def adding_collection_to_mongodb():
    mycollection_1 = my_mongo_database['channel_details']
    mycollection_1.insert_many(get_all_channels_data(channel_id))
    mycollection_2 = my_mongo_database['video_details']
    mycollection_2.insert_many(get_video_details(video_ids))
    # mycollection_3 = my_mongo_database['comment_details']
    # mycollection_3.insert_many(comments())

if selected == 'Upload data to MongoDB':
    st.header("*****Enter your Channel_Id*****")
    channel_id = st.text_input(' ').split(',')
    if channel_id and st.button('Show Channel Details'):
        st.write(f'#### Channel Details from :red["{get_all_channels_data(channel_id)[0]["Channel_name"]}"] channel')
        st.table(get_all_channels_data(channel_id)[0])
    if st.button('Upload Data to MongoDB'):
        with st.spinner('Working on it...'):
            adding_collection_to_mongodb()
            st.success("Upload to MongoDB successful !!")
if selected == 'SQL Data Transform':
    st.subheader('Select channel for Data Transform')
    Input = st.selectbox("Select channel",options = fetch_channel_name())
    def channels_insertion():
                col_1 =  my_mongo_database['channel_details']
                query_1 = "insert into channels(Channel_id,Channel_name,Playlist_id,Subscribers,Views,Total_videos,Description,Country) values(%s,%s,%s,%s,%s,%s,%s,%s)"         
                for channel in col_1.find({"Channel_name" : Input},{'_id' : 0}):
                    mycursor.execute(query_1,tuple(channel.values()))
                    my_sql_connection.commit()

    def videos_insertion():
                col_2 = my_mongo_database['video_details']
                query_2 = "insert into videos(Channel_name,Channel_id,Video_id,Title,Thumbnail,Description,Published_date,Duration,Views,Likes,Comments,Favorite_count,Definition,Caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                for video in col_2.find({"Channel_name" : Input},{'_id' : 0}):
                    mycursor.execute(query_2,tuple(video.values()))
                    my_sql_connection.commit()

    def comments_insertion():
                col_2 = my_mongo_database['video_details']
                col_3 = my_mongo_database['comments_details']
                query_3 = "insert into comments(Comment_id,Video_id,Comment_text,Comment_author,Comment_posted_date,Like_count,Reply_count) values(%s,%s,%s,%s,%s,%s,%s)"
                for videos in col_2.find({"Channel_name" : Input},{'_id' : 0}):
                    for comment in col_3.find({'Video_id': videos['Video_id']},{'_id' : 0}):
                        mycursor.execute(query_3,tuple(comment.values()))
                        my_sql_connection.commit()

    if st.button("Submit"):
        channels_insertion()
        videos_insertion()
        comments_insertion()
        st.success("Transform to sql is done successfully !!")
if selected == 'Queries':
    def name_of_all_videos():
        mycursor.execute("select title as Video_Title, channel_name as Channel_Name from videos order by channel_name")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    def channel_having_most_videos():
        mycursor.execute("select channel_name as Channel_Name, total_videos as Total_videos from channels order by total_videos")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,x=mycursor.column_names[0],
                        y=mycursor.column_names[1],
                        color=mycursor.column_names[0]
                        )
        st.plotly_chart(fig)
    def top_10_most_viwed_videos():
        mycursor.execute("select channel_name as Channel_Name,title AS Video_Title,views as Views from videos order by views DESC LIMIT 10")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,x=mycursor.column_names[2],
                        y=mycursor.column_names[1],
                        orientation='h',
                        color=mycursor.column_names[0]
                        )
        st.plotly_chart(fig,use_container_width=True)
    def number_of_comments():
        mycursor.execute("select a.video_id as Video_id, a.title as Video_Title, b.Total_Comments from videos as a left join (select video_id,COUNT(comment_id) as Total_Comments from comments group by video_id) as b on a.video_id = b.video_id order by b.Total_Comments DESC")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    def highest_number_of_likes():
        mycursor.execute('select channel_name as Channel_Name,title AS Title,likes AS Likes_Count from videos order by likes DESC LIMIT 10')
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,x=mycursor.column_names[2],
                        y=mycursor.column_names[1],
                        orientation='h',
                        color=mycursor.column_names[0]
                        )
        st.plotly_chart(fig,use_container_width=True)
    def total_number_of_likes_and_dislike():
        mycursor.execute("select title AS Title, likes AS Likes_Count from videos order by likes DESC")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    def total_number_of_views():
        mycursor.execute("select channel_name as Channel_Name, views as Views from channels order by views DESC")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,x=mycursor.column_names[0],
                        y=mycursor.column_names[1],
                        orientation='v',
                        color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
    def names_of_all_the_channels():
        mycursor.execute("select channel_name AS Channel_Name from videos where published_date like '2022%' group by channel_name ORDER BY channel_name")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    def avg_duration_of_all_videos():
        mycursor.execute("select channel_name AS Channel_Name, AVG(Duration) AS Average_Video_Duration from videos group by channel_name order by AVG(Duration) DESC")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,x=mycursor.column_names[0],
                        y=mycursor.column_names[1],
                        color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
    def highest_num_comments():
        mycursor.execute("select channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments from videos order by comments")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,x=mycursor.column_names[1],
                        y=mycursor.column_names[2],
                        orientation='v',
                        color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
    questions = st.selectbox('Questions',
        ['1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])     
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        name_of_all_videos()
    
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        channel_having_most_videos()
    
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        top_10_most_viwed_videos()
       
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        number_of_comments()
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        highest_number_of_likes()
                   
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        total_number_of_likes_and_dislike()
                
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        total_number_of_views()
                
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        names_of_all_the_channels()
                
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        avg_duration_of_all_videos()
                
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        highest_num_comments()