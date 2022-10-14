import sys
import spotipy
import yaml
import spotipy.util as util
from pprint import pprint
import random
import datetime
import pandas as pd
import time
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3

## get secret variables
def load_config():
    # in order to keep private spotify client_id and client_secret from being hard-coded in the script,
    # i put them in a separate file that gets read by this function
    global user_config
    stream = open('config.yaml')
    user_config = yaml.safe_load(stream)

## get list of songs via albums
def get_artist_id(artist_name: str) -> str:
    results = sp.search(q = f'artist: {artist_name}', type = 'artist')

    if results['artists']['total']:
        artist_id = results['artists']['items'][0]['id']

    return artist_id

## get a dictionary of talking heads album ids and names
def get_albums(artist_id: str) -> dict:

    album_dict = {}

    # artist_albums is a spotipy method that gets an artist's albums
    # album_type='album' excludes EPs, compilations, etc.
    albums = sp.artist_albums(artist_id,album_type='album')

    # the response of artist_albums is a dictionary
    for album in albums['items']:
        album_dict[album['id']] = album['name']

    return album_dict

## filter albums based on known "duplicates" (deluxe/expanded versions)
def filter_albums(album_dict: dict) -> list:
    all_album_names = [album for id, album in album_dict.items()]
    all_album_ids = [album_id for album_id in album_dict]

    bad_album_ids = []

    for id, name in album_dict.items():

        # first check for deluxe versions of albums where "non-deluxe" version exists
        if name[-15:-1] == 'Deluxe Version' and name[:-17] in all_album_names:
            bad_album_ids.append(id)
        
        # then check for remastered versions where non-remastered version exists
        # part 1: (YYYY Remaster)
        if name[-8:-1] == 'Remaster' and name[:-16] in all_album_names:
            bad_album_ids.append(id)
        # part 1: (Expanded YYYY Remaster)
        if name[-8:-1] == 'Remaster' and name[:-25] in all_album_names:
            bad_album_ids.append(id)

    filtered_album_ids = list(set(all_album_ids).difference(bad_album_ids))
    
    return filtered_album_ids

## get a dictionary of song uri and name
def get_songs(albums: list) -> dict:

    songs = [sp.album_tracks(album) for album in albums]

    song_dict = {}
    for song in songs:
        for item in song['items']:
            song_dict[item['uri']] = item['name']
            
    return song_dict

## get songs to put on playlist
# todo: pass variable for number of songs, use 10 as default
def get_ten_random(song_dict: dict) -> dict:

    return {k: song_dict[k] for k in random.sample(list(song_dict),10)}

## create playlist and return its name
def create_playlist() -> str:
    # gather the bits and pieces of the playlist details and then create the playlist!
    date = datetime.datetime.now().strftime('%m/%d/%Y')
    playlist_name = f'The name of this playlist is Talking Heads: {date}'
    desc = f'Ten random Talking Heads songs to get you through the day: {date}'

    # for now the playlist has to be public so we can get the uri later
    sp.user_playlist_create(user=user_config['username_words'],name =playlist_name,public=True,collaborative=False,description=desc)

    return playlist_name

def make_playlist_private(playlist_id):
    sp.playlist_change_details(playlist_id, public=False)

def get_playlist_id(playlist_name) -> str:
    playlists = sp.current_user_playlists()

    for item in playlists['items']:        
        if item['name'] == playlist_name:
            return item['id']

def add_songs(playlist_id, uris_to_add_list: list):
    sp.user_playlist_add_tracks(user=user_config['username'], playlist_id=playlist_id, tracks=uris_to_add_list)

## dataframe stuff for adding playlist to sqlite db
def create_playlist_dataframe(song_names: list, song_uris: list, playlist_id: str):
    playlist_dict = {
        "song_name" : song_names
        ,"song_uri" : song_uris
        ,"artist_name" : 'Talking Heads'
        ,"playlist_id" : playlist_id
        ,"date" : datetime.datetime.now().strftime('%m/%d/%Y')
        ,"creation_id" : int(round(time.time() * 1000))
    }
    
    song_df = pd.DataFrame(playlist_dict, columns = ["song_name", "song_uri", "artist_name", "playlist_id", "date", "creation_id"])

    return song_df

## validation
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs in playlist. Finishing execution")
        return False 

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True

## entire ETL process
def run_etl():
    global sp
    global user_config
    global artist_name
    
    # get private spotify info from config.yaml
    load_config()

    token = util.prompt_for_user_token(
        user_config['username'],
        scope='playlist-modify-private,playlist-modify-public',
        client_id=user_config['client_id'],
        client_secret=user_config['client_secret'],
        redirect_uri=user_config['redirect_uri']
        )

    if token:
        # create spotify object
        sp = spotipy.Spotify(auth=token)

        # set artist name
        artist_name = 'Talking Heads'

        # get artist id
        artist_id = get_artist_id(artist_name)

        # store albums in a variable
        album_dict = get_albums(artist_id)

        # run this step if you want to filter out "duplicate" albums (deluxe versions, remastered versions)
        # where exists a "non-deluxe" or "non-remastered" version
        filtered_album_ids = filter_albums(album_dict)

        # store songs in a dictionary {uri : name}
        song_dict = get_songs(filtered_album_ids)

        # store songs to add in a variable
        songs_to_add_dict = get_ten_random(song_dict)
        uris_to_add_list = [k for k in songs_to_add_dict]
        songs_to_add_list = [songs_to_add_dict[k] for k,i in songs_to_add_dict.items()]

        # create a playlist and store its name in a variable
        playlist_name = create_playlist()

        # get playlist id for playlist you just created, so that you can add songs to it
        playlist_id = get_playlist_id(playlist_name)

        # add songs!
        add_songs(playlist_id,uris_to_add_list)

        # optional: remove playlist from spotify profile
        make_playlist_private(playlist_id)     

        song_df = create_playlist_dataframe(songs_to_add_list,uris_to_add_list,playlist_id)   


        # Load

        engine = sqlalchemy.create_engine(user_config['database_location'])
        conn = sqlite3.connect('talk_to_these_heads.sqlite')
        cursor = conn.cursor()

        sql_query = """
        CREATE TABLE IF NOT EXISTS my_playlists(
            song_name VARCHAR(200),
            song_uri VARCHAR(200),
            artist_name VARCHAR(200),
            playlist_id VARCHAR(200),
            date VARCHAR(200),
            creation_id VARCHAR(200)
            
        )
        """       

        cursor.execute(sql_query)
        print('Database opened successfully')

        try:
            song_df.to_sql('my_playlists', engine, index=False, if_exists='append')
        except:
            print('Data already exists in the database')

        conn.close()
        print('Database closed successfully')
        
    else:
        print ("Can't get token for", user_config['username'])


if __name__ == '__main__':
    run_etl()