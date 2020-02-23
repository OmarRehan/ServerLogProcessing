import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
import logging
import sys
import shutil

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

def FUNC_ProcessSongData(param_df):
    """A Function to Process Song Table Data, recieves a Dataframe then selects, processes & returns Song Data ready to be inserted in the Db"""
    try:
        DF_SongData = param_df[['song_id','title','artist_id','year','duration']].copy()
        DF_SongData.year = DF_SongData.year.apply(lambda x: -9999 if x in (0,np.nan) else x)# Changing 0 years into the default value of missing numeric value -9999
        
        return DF_SongData
    except Exception as e:
        logging.error(" {}".format(e))
        raise Exception("Failed To Process Song Table Data") 
        
        
        
def FUNC_ProcessArtistData(param_df):
    """A Function to Process Artist Table Data, recieves a Dataframe then selects, processes & returns Song Data ready to be inserted in the Db"""
    
    try:
        DF_ArtistData = param_df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].copy()
        
        DF_ArtistData.artist_location = DF_ArtistData.artist_location.apply(lambda x: 'Unknown' if (x == "" or None) else x)# Changing 0 years into the default value of missing numeric value -9999
        DF_ArtistData.artist_latitude.replace(np.nan,-9999, inplace=True)# Replacing Nulls in artist_latitude with the default value of missing numeric value -9999
        DF_ArtistData.artist_longitude.replace(np.nan,-9999, inplace=True)# Replacing Nulls in artist_longitude with the default value of missing numeric value -9999
        
        return DF_ArtistData
    except Exception as e:
        logging.error(" {}".format(e))
        raise Exception("Failed To Process Artist Table Data") 
        
        
def FUNC_ProcessTimestampData(param_df,param_col_name):
    """Recieves a Data Frame contains a single Columns of Unix time stamp and returns a DataFrame Contains all TIME_TBL Columns"""
    try :
        DF_TimeStamp = param_df[[param_col_name]].copy()
        DF_TimeStamp["TS_FORMATTED"] = pd.to_datetime(param_df[param_col_name],unit='ms')
        DF_TimeStamp["YEAR_COL"] = pd.DatetimeIndex(DF_TimeStamp.TS_FORMATTED).year
        DF_TimeStamp["MONTH_COL"] = pd.DatetimeIndex(DF_TimeStamp.TS_FORMATTED).month
        DF_TimeStamp["DAY_COL"] = pd.DatetimeIndex(DF_TimeStamp.TS_FORMATTED).day
        DF_TimeStamp["HOUR_COL"] = pd.DatetimeIndex(DF_TimeStamp.TS_FORMATTED).hour
        DF_TimeStamp["MINUTE_COL"] = pd.DatetimeIndex(DF_TimeStamp.TS_FORMATTED).minute
        DF_TimeStamp["DAY_NAME"] = pd.DatetimeIndex(DF_TimeStamp.TS_FORMATTED).day_name()
        
        DF_TimeStamp.rename(columns={'ts':'TIME_ID'} , inplace=True)
        
        DF_TimeStamp.drop_duplicates('TIME_ID',inplace=True)
        
        return DF_TimeStamp
    except Exception as  e:
        logging.error(" {}".format(e))
        raise Exception("Failed To Process Time Table Data") 
        

def FUNC_ProcessUserData(param_df):
    """A Function to Process User Table Data, recieves a Dataframe then selects, processes & returns User Data ready to be inserted in the Db"""
    try:
        
        # Filtering on Logged user only, Sorting the Data to preserve the latest Status while removing duplicates & Selecting the Required Columns
        DF_UserData = param_df[param_df.auth== 'Logged In'][['userId','firstName','lastName','gender','level','ts']].copy()
        DF_UserData.sort_values('ts',inplace=True)
        DF_UserData.drop(['ts'], axis=1, inplace=True)
        DF_UserData.userId = DF_UserData.userId.astype('int')
        
        DF_UserData.drop_duplicates('userId',inplace=True,keep='last')
        
        return DF_UserData
    except Exception as e:
        logging.error(" {}".format(e))
        raise Exception("Failed To Process User Table Data")

        
def FUNC_ProcessSongPlayTempData(param_df):
    """A Function to Process Song Play Table Data, recieves a Dataframe then selects, processes & returns Song Play Data ready to be inserted in the Db"""
    try:
        DF_SongPlayData = param_df[param_df.page == 'NextSong'] #Filtering on 'NextSong' because this is where Song Data Available
        DF_SongPlayData = DF_SongPlayData[['ts','sessionId','artist','song','userId','level','location','userAgent','auth','length']].copy().drop_duplicates(['ts','sessionId'])
        DF_SongPlayData.userId = DF_SongPlayData.userId.apply(lambda x: -9999 if x in (np.nan,'') else x)
        
        return DF_SongPlayData
    except Exception as e:
        logging.error(" {}".format(e))
        raise Exception("Failed To Process Temp Song Play Table Data") 


def FUNC_TruncateTempSongPlay(cur,conn):
    try:
        cur.execute("TRUNCATE TABLE temp_song_play_tbl;")
        conn.commit()
        logging.info(" Temp Song Play Table Has been Truncated".format(cur.rowcount))
        
        return True
    except Exception as e:
        logging.error(" Failed to Truncate Temp Song Play Table. {}".format(e))
        raise Exception("Failed To Truncate Temp Song Play Table.") 


def process_song_file(cur, filepath):
    try :
        # open song file
        df = pd.read_json(filepath,lines=True)
    
    except Exception as e:
        logging.error(" Failed to open the file. {}".format(e))
        raise Exception("Failed to open Song file.") 

    try:
        # insert song record
        cur.executemany(song_table_insert, FUNC_ProcessSongData(df).values)
        logging.info(" ({}) Rows Inserted/Updated Into Song Table ".format(cur.rowcount))

        # insert artist record
        cur.executemany(artist_table_insert, FUNC_ProcessArtistData(df).values)
        logging.info(" ({}) Rows Inserted/Updated Into Artist Table ".format(cur.rowcount))
        
        return True
    
    except Exception as e:
        logging.error(" Failed to process Song Data. {}".format(e))
        return False

        
        
def process_log_file(cur, filepath):
    
    try:
        # open log file
        df = pd.read_json(filepath,lines=True)
        
    except Exception as e:
        logging.error(" Failed to open the file. {}".format(e))
        raise Exception("Failed to open Log file.") 

    try:
        #convert timestamp column to datetime then get time data records and insert them
        cur.executemany(time_table_insert, FUNC_ProcessTimestampData(df,'ts').values)
        logging.info(" ({}) Rows Inserted/Updated Into Time Table ".format(cur.rowcount))


        # get user data and then insert them insert user records
        cur.executemany(user_table_insert, FUNC_ProcessUserData(df).values)
        logging.info(" ({}) Rows Inserted/Updated Into User Table ".format(cur.rowcount))

        # get Song Play Data and insert it into Temp Song Play Table
        cur.executemany(songplay_temp_table_insert,FUNC_ProcessSongPlayTempData(df).values)
        logging.info(" ({}) Rows Inserted Into Temp Song Play Table ".format(cur.rowcount))
        
        return True
    except Exception as e:
        logging.error(" Failed to process Log Data. {}".format(e))
        return False


def FUNC_MoveProcessedFile(param_filepath):
    """
    Moves the Data files to Archive Directory to avoid Reprocessing them again, Currently Copying to preserve Demo Schema
    """
    try:
        
        if os.path.isfile(param_filepath):
            LIST_ = param_filepath.split('/')
            
            STR_TrgtPath = '/home/workspace/LOADED_DATA'
            
            if 'log_data' in LIST_:
                STR_TrgtPath = os.path.join(STR_TrgtPath,'log_data')
                STR_TrgtFile = os.path.join(STR_TrgtPath,param_filepath.split('/')[-1])
            else:
                STR_TrgtPath = os.path.join(STR_TrgtPath,'song_data')
                STR_TrgtFile = os.path.join(STR_TrgtPath,param_filepath.split('/')[-1])
            
            if not os.path.isdir(STR_TrgtPath):
                os.makedirs(STR_TrgtPath)  
            
            #Currently it is copy to be able to rerun the Code againg without moving the data files
            #os.replace(param_filepath, STR_TrgtFile)
            shutil.copyfile(param_filepath, STR_TrgtFile)
            
        else:
            raise Exception("File Does Not Exist")
            
    except Exception as e:
        logging.error(" Failed While Moving Data Files. {}".format(e))
        
        
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    logging.info(' {} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(sorted(all_files), 1):
        if func(cur, datafile):
            conn.commit()
            logging.info(' {} Processed , {}/{} files processed.'.format(datafile,i, num_files))
            FUNC_MoveProcessedFile(datafile)

        else:
            logging.warning(' Failed To Process {} , {}/{} files processed.'.format(datafile,i, num_files))

        

def FUNC_MergeSongPlayData(cur, conn):
    
    try:
        
        cur.execute(songplay_table_insert)
        conn.commit()
        logging.info(" ({}) Rows Inserted/Updated Into Song Play Table ".format(cur.rowcount))
        
    except Exception as e:
        logging.error(" Failed to Merge Song Play Data. {}".format(e))

        
        
def main():
    
    
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()

    except (psycopg2.Error, Exception) as e:
        logging.error(" Exception While Connecting to the Db:", e)
    
    else:
        DICT_ConnInfo = conn.get_dsn_parameters()

        logging.info(" Connected to {}, Host: {}, User: {}".format\
                (
                DICT_ConnInfo.get('dbname')
                , DICT_ConnInfo.get('host')
                , DICT_ConnInfo.get('user')
            )
        )
        
        try:
            
            process_data(cur, conn, filepath='data/song_data', func=process_song_file)
            FUNC_TruncateTempSongPlay(cur,conn)
            process_data(cur, conn, filepath='data/log_data', func=process_log_file)
            FUNC_MergeSongPlayData(cur, conn)
            
        except Exception as e:
            logging.error(" {}:", e)
        
        finally:
            conn.close()
        


if __name__ == "__main__":
    main()