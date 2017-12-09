import mysql.connector 
import hdf5_getters
import os
import glob

HOSTNAME = '127.0.0.1'
USERNAME = 'root'
PASSWORD = ''
DATABASE = 'millionsong'
SKIPCOUNT = 561000
BASEDIR = '/home/ec2-user/mount_point/data'
myConnection = mysql.connector.connect(host=HOSTNAME, user=USERNAME, passwd=PASSWORD, db=DATABASE)

def import_data(basedir,SKIPCOUNT, ext='.h5') :
    years = []
    durations = []
    ids = []
    count = 0
    duplicates = 0

    cursor = myConnection.cursor()
    cursor.execute("SET autocommit = 0;")
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root,'*'+ext))
        for f in files:
            if SKIPCOUNT > 0:
                SKIPCOUNT = SKIPCOUNT - 1
                if SKIPCOUNT % 1000 == 0:
                    print("Remaining skips: {0}".format(SKIPCOUNT))
                continue
            h5 = hdf5_getters.open_h5_file_read(f)
            try: 
                for idx in range(len(h5.root.musicbrainz.songs.cols.year)):
                    song_year = hdf5_getters.get_year(h5,idx)
               
                    song_duration = hdf5_getters.get_duration(h5, idx)
               
                    song_id = hdf5_getters.get_song_id(h5, idx)
            except Exception as e:
                    print(e)
                    continue
            h5.close()
            
            try:
                number_of_rows = cursor.execute("INSERT INTO  millionsong.song VALUES('{0}',{1},'{2}')".format(song_id, song_duration,song_year))

               
                count = count + 1
                
            except mysql.connector.errors.IntegrityError:
                duplicates = duplicates + 1               
                if duplicates % 100 == 0:
                    print("Duplicate: {0}".format(duplicates))
                continue       
                
            
            if count %  500 == 0:
                myConnection.commit()
                print("On iteration {0}".format(count))
    return

import_data(BASEDIR, SKIPCOUNT)
myConnection.close()
