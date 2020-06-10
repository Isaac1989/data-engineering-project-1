import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np
import datetime



def process_song_file(cur, filepath):
    """
    Function reads a file from the `song_data` in the `data` processes and loads into the coressponding table
    
    inputs: cur ---- A cursor object after connecting to a Postgres database
    
            filepath: a string of file path
    output: None
    """
    # open song file
    df = df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = [x for x in df[['song_id','title','artist_id','year','duration']].values.squeeze()]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    temp = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.squeeze()
    for i,x in enumerate(temp):
        try:
            if np.isnan(x):
                temp[i] = ''
        except TypeError:
            pass
    artist_data = [x for x in temp]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Function reads a file from the `log_data` in the `data` processes and loads into the coressponding table
    
    inputs: cur ---- A cursor object after connecting to a Postgres database
            filepath: a string of file path
    output: None
    """
    def extract(time):
        """ This just utility function that takes datetime object and parses out the various component.
            It returns a tuple
            
            input: datetime
            -----
            output: tuple
            ------
        """
        hour = time.hour
        day = time.day
        week  = time.isocalendar()[1]
        month= time.month
        year = time.year
        weekday = time.isocalendar()[2]
        return time, hour, day, week, month, year, weekday


    def makedf(S, col):
        """The function takes a pandas Series of tuple and converts it into a
            pandas DataFrame object with columns `col`.
        inputs: S, pandas Series
                col: columns for the output DataFrame
        outputs:
                DataFrame object
        """
        data = []
        for x in S.values:
            data.append(x)
        return pd.DataFrame(data, columns = col)

    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']
    # convert timestamp column to datetime
    df['ts'] = df['ts'].apply(lambda x : datetime.datetime.fromtimestamp(x//1000.0))
    t = df['ts']
    
    # insert time data records
    time_data = t.apply(extract)
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    time_df = makedf(time_data,column_labels )


    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName', 'gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplayid = index
        ts,userid,level, sessionid = row.ts, row.userId, row.level, row.sessionId
        location,user_agent = row.location, row.userAgent 
    
        songplay_data = (songplayid, ts, userid, level, songid, artistid, sessionid, location, user_agent)
        
        cur.execute(songplay_table_insert, songplay_data)
        
        
def process_data(cur, conn, filepath, func):
    """
    Function process every json file in folder `data`. 
    1. Reads json files
    2. Passes filepath to the `func` to do the processing
    
    inputs: 
        cur: A cursor object from a postgres connection
        conn: a conection object after connecting to the postgres database
        filepath: a string object of the path to the folder housing data to processed
        func: function object for ETL process.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
        
def main():
    """This function is where every thin comes together.
        1. It connects to a database
        2. call the functions `process_data` with appropriate arguments to start
           the ETL process.
        inputs: None
        outputs: None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
