import re
import requests
import pandas as pd
import spotipy as sp
from spotipy.oauth2 import SpotifyClientCredentials
from bs4 import BeautifulSoup
from bs4 import BeautifulSoup as Soup, Tag
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
spotify = sp.Spotify(client_credentials_manager=SpotifyClientCredentials())
yes_id = spotify.search('Yes', limit=1, offset=0, type='artist', market=None)['artists']['items'][0]['id']
url = 'https://en.wikipedia.org/wiki/Yes_(band)'
SERVER='postgres'
USERNAME='postgres'
PASSWORD='postgres'
PORT=5432
CONN = f"postgresql://{USERNAME}:{PASSWORD}@{SERVER}:{PORT}"

def get_studio_albums():
    html = requests.get(url)
    soup = BeautifulSoup(html.text, 'html.parser')

    p = soup.find_all("b", string="Studio albums")[0].parent.next_sibling
    ul = p.findNext('ul')

    albums = []
    years = []
    for item in ul.findAll('li'):
        if isinstance(item, Tag):
            year = re.search("\(\d+\)", item.text)[0].replace('(','').replace(')','')
            album = item.text[:item.text.find('(')].strip()
            years.append(year)
            albums.append(album)

    return pd.DataFrame({'album':albums, 'year':years})   
    

def album_artist_search(name, year):
    offset = 0
    while True:
        album_id = None
        search_result = spotify.artist_albums(artist_id=yes_id,album_type='album',limit=50, offset=offset)
        for tmp in search_result['items']:
            if name.strip() == tmp['name'] and tmp['release_date'][:4]==year:
                album_id = tmp['id']
        if album_id:
            return album_id
        offset += 50
        if offset > 500:
            break
            
    return None
            
def album_general_search(name, year):
    offset = 0
    while True:
        album_id = None
        search_result = spotify.search(name, limit=50, offset=0, type='album', market=None)
        for tmp in search_result['albums']['items']:
            if tmp['artists'][0]['id']==yes_id and tmp['release_date'][:4]==year:
                album_id = tmp['id']
        if album_id:
            return album_id
        offset += 50
        if offset > 500:
            break
                
    return None


def find_album(name, year):
    album_id = album_artist_search(name, year)
    if album_id:
        return album_id
    return album_general_search(name, year)


def get_tracks(album_id):
    track_names = []
    track_ids = []
    for i in spotify.album_tracks(album_id)['items']:
        track_names.append(i['name'])
        track_ids.append(i['id'])
    return track_names, track_ids

def main():
    yes_df = get_studio_albums()
    ids = []

    for row in yes_df.iterrows():
        album_id = find_album(row[1]['album'], row[1]['year'])
        ids.append(album_id)
        
    yes_df['album_id'] = ids
    yes_df = yes_df.dropna()

    tracks_df = pd.DataFrame()

    for row in yes_df.iterrows():
        album_id = row[1]['album_id']
        track_names, track_ids = get_tracks(album_id)
        if tracks_df.shape[0] == 0:
            tracks_df['track_id'] = track_ids
            tracks_df['track_name'] = track_names
            tracks_df['album_id'] = album_id
            continue
        tmp = pd.DataFrame()
        tmp['track_id'] = track_ids
        tmp['track_name'] = track_names
        tmp['album_id'] = album_id
        tracks_df = pd.concat([tracks_df, tmp])

    features = ['danceability', 'energy', 'key', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms','time_signature']

    features_dict = {}
    for row in tracks_df.iterrows():
        track_id = row[1]['track_id']
        features_dict[track_id] = spotify.audio_features(track_id)[0]

    for f in features:
        tracks_df[f] = tracks_df['track_id'].apply(lambda x: features_dict[x][f])

    df = tracks_df.merge(yes_df, on='album_id')

    engine = create_engine(CONN)
    df.to_sql('yes_tbl', con=engine, if_exists='replace', index=False)
    engine.dispose()
    print('PROCESS COMPLETE', flush=True)

if __name__ == '__main__':

    main()