import mysql.connector 
import hdf5_getters
import os
import glob

HOSTNAME = '127.0.0.1'
USERNAME = 'root'
PASSWORD = 'operam'
DATABASE = 'millionsong'
BASEDIR = '/home/ec2-user/mount_point/data'
myConnection = mysql.connector.connect(host=HOSTNAME, user=USERNAME, passwd=PASSWORD, db=DATABASE)

def import_data(basedir,ext='.h5') :
    years = []
    durations = []
    ids = []
    count = 0

    cursor = myConnection.cursor()
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root,'*'+ext))
        for f in files:
            songidx = 0
            h5 = hdf5_getters.open_h5_file_read(f)
            for idx in range(len(h5.root.musicbrainz.songs.cols.year)):
                song_year = hdf5_getters.get_year(h5,idx)
               # print(song_year)
                song_duration = hdf5_getters.get_duration(h5, idx)
               # print(song_duration)
                song_id = hdf5_getters.get_song_id(h5, idx)
               # print(song_id)
            h5.close()
            
            number_of_rows = cursor.execute("INSERT INTO  millionsong.song VALUES('{0}',{1},'{2}')".format(song_id, song_duration,song_year))

            myConnection.commit()   # you need to call commit() method to save your changes to the database
            print(number_of_rows)
            count = count + 1
            if count %  1000:
                print("On iteration {0}".format(count))
    return

import_data(BASEDIR)
myConnection.close()
