import mysql.connector # connection driver
import hdf5_getters # this is the helper module created by Thierry to access the H5 files
import os # for file system IO handling
import glob

# default configurations (in a production setting, I'd deploy these to an external config.yaml so that our DevOps engineers can quickly
# make changes without redeployment)
HOSTNAME = '127.0.0.1'
USERNAME = 'root'
PASSWORD = ''
DATABASE = 'millionsong'
BASEDIR = '/home/ec2-user/mount_point/data'

myConnection = mysql.connector.connect(host=HOSTNAME, user=USERNAME, passwd=PASSWORD, db=DATABASE)

def import_data(basedir, ext='.h5') :
    count = 0
    duplicates = 0

    cursor = myConnection.cursor()
    cursor.execute("SET autocommit = 0;") # MySQL defaults to setting autcommit to TRUE

	# SKIPCOUNT to skip uploaded files and expedite the load process. The SKIPCOUNT was set to the number of rows
	# in the database table already. For example, if the table contains 500,000 rows, we know that we can skip the first 500,000 files
	# since recursive file iteration pattern is deterministic (ie. the same files will be iterated through in the same order each time)
	# This helps expedite the insertion/load process in the case where your script terminates and you don't want to have to your
	# file iteration process completely over.
    SKIPCOUNT = cursor.fetchone("SELECT COUNT(*) FROM song;")

    for root, dirs, files in os.walk(basedir): # iterate through each file within the mounted file dataset
        
        files = glob.glob(os.path.join(root,'*'+ext))
        
        for f in files:

            if SKIPCOUNT > 0:
                SKIPCOUNT = SKIPCOUNT - 1
                if SKIPCOUNT % 1000 == 0:
                    print("Remaining skips: {0}".format(SKIPCOUNT))
                continue 

            h5 = hdf5_getters.open_h5_file_read(f) # use helper module to open the HDF5 file

            try: # wrapped in try / except clause since some H5 files do not have .cols or .year!
                
                for idx in range(len(h5.root.musicbrainz.songs.cols.year)):
                    song_year = hdf5_getters.get_year(h5,idx)
                    song_duration = hdf5_getters.get_duration(h5, idx)
                    song_id = hdf5_getters.get_song_id(h5, idx)
            
            except Exception as e:
                    print(e) # we'll run this script using nohup so any errors will be outputted to a nohup.out server log for us to debug
                    continue
            
            h5.close()
            
            try:
                number_of_rows = cursor.execute("INSERT INTO  millionsong.song VALUES('{0}',{1},'{2}')".format(song_id, song_duration,song_year))
                count = count + 1
                
            except mysql.connector.errors.IntegrityError: # MySQL will throw this error when it runs into a duplicate primary key
                duplicates = duplicates + 1               
                if duplicates % 100 == 0:
                    print("Duplicate: {0}".format(duplicates))
                continue       
                            
            if count %  500 == 0:
                myConnection.commit() # commit transactions every 500 insertions (committing after each transaction is too expensive and unnecessary!)
                print("On iteration {0}".format(count))
    return

import_data(BASEDIR)
myConnection.close()