import sys
import os
import getopt
import string
import mysql.connector
import ConfigParser

def db_load_file(connection, path):

    try:
        with open(path): pass
    except IOError:
        print 'File \'{}\' does not exist'.format(path)
        return

    print "Loading: {}...".format(path)

    filename = os.path.basename(path)
    fields = string.split(filename, '-')

    cursor = connection.cursor()

    #add the media source
    add_media_source = ("INSERT INTO `media_source` (reputation, name, date_acquired, os) "
                        "VALUES(0, %(name)s, %(date_acquired)s, %(os)s) ")

    data_media_source = {

        'name':fields[2],
        'date_acquired':fields[0],
        'os':fields[1],
    }

    try:

        cursor.execute(add_media_source, data_media_source)
        connection.commit()
    except mysql.connector.Error as err:
        print(err)
        return

    media_source_id = cursor.lastrowid

    #load raw csv into the staging table from the client
    add_staging_table = ("LOAD DATA LOCAL INFILE %(path)s INTO TABLE `staging_table` "
                           "FIELDS TERMINATED BY ',' "
                           "LINES TERMINATED BY '\n';")
    data_staging_table = {
        'path': path,
    }
    
    try:

        cursor.execute(add_staging_table, data_staging_table)
        connection.commit() 
   
    except mysql.connector.Error as err:
        print(err)
        return

    cursor.callproc('map_staging_table', (media_source_id,))
    cursor.execute("DELETE FROM `staging_table`;")
    connection.commit()
    print "Completed import of {}".format(path)


def run(cnx, path):

    if(os.path.isfile(path)):
        db_load_file(cnx, path)
    elif(os.path.isDir(path)):
        for r, d, f in os.walk(path):
            while d > 0:
                d.pop()
            for file in f:
                if not file.startswith('.'):
                    os.path.abspath(os.path.join(r, file))
                    db_load_file(connection, input_dir + "/" + file)
    else:
        print 'Please input a file or a directory for import'
