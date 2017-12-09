import mysql.connector 
import hdf5_getters
import os
import glob

BASEDIR = '/home/ec2-user/mount_point/data'

def import_data(basedir,ext='.h5') :
    count = 0
    duplicates = 0
    song_duration_dict = {}
    song_count = {}
    song_durations_avg = {}
    
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root,'*'+ext))
        for f in files:
            h5 = hdf5_getters.open_h5_file_read(f)
            for idx in range(len(h5.root.musicbrainz.songs.cols.year)):
                song_year = hdf5_getters.get_year(h5,idx)
               
                song_duration = hdf5_getters.get_duration(h5, idx)
                if song_year in song_duration_dict.keys():
                    song_duration_dict[song_year] += song_duration
                else:
                    song_duration_dict[song_year] = song_duration

                if song_year in song_count:
                    song_count[song_year] += 1
                else:
                    song_count[song_year] = 1

            h5.close()
            
            count = count + 1
                
            if count %  100 == 0:
                print("On iteration {0}".format(count))
                
                for year in song_count.keys():
                    song_durations_avg[year] = round(song_duration_dict[year] / song_count[year],2)               
                print(song_durations_avg)

    
    return

import_data(BASEDIR)
