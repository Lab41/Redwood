import sys
import os
import getopt
import string
import time
from datetime import datetime
import MySQLdb

def db_load_file(connection, path):

    try:
        with open(path): pass
    except IOError:
        print 'File \'{}\' does not exist'.format(path)
        return
    
    start_time = time.time()
    print "Loading: {}...".format(path)

    filename = os.path.basename(path)
    fields = string.split(filename, '-')

    if(len(fields) != 3):
        print "Error: Improper naming scheme"
        return
    cursor = connection.cursor()
    os_id = None



    #transaction for adding to media and os tables. Both succeed or both fail
    try:

        data_os = {
            'name':fields[1],
        }

        #add os 
        add_os = ("INSERT INTO `os` (name) VALUES('%(name)s') ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)") % data_os
        
        cursor.execute(add_os)
        
        connection.commit()

    
        
    except MySQLdb.Error, e:
        if connection:
            connection.rollback()                       
            print "Error %d: %s" % (e.args[0],e.args[1])
            return                                        

    os_id = cursor.lastrowid
    
    if(os_id is None):
        print "Unable to find corresponding os"
        return

    try:
        date_object = datetime.strptime(fields[0], '%m%d%Y')

        data_media_source = {

            'name':fields[2],
            'date_acquired':date_object.isoformat(),
            'os_id':os_id,
        }

        #add the media source
        add_media_source = ("INSERT INTO `media_source` (reputation, name, date_acquired, os_id) "
                            "VALUES(0, '%(name)s', '%(date_acquired)s', '%(os_id)s') ") % data_media_source
        
        cursor.execute(add_media_source)
        connection.commit()

    except MySQLdb.Error, e:
        if connection:
            connection.rollback()                       
            print "Error %d: %s" % (e.args[0],e.args[1])
            return                                        

    media_source_id = cursor.lastrowid
    
      

    #load raw csv into the staging table from the client
    add_staging_table = ("LOAD DATA LOCAL INFILE '{}' INTO TABLE `staging_table` "
                         "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' "
                         "IGNORE 1 LINES "
                         "(contents_hash, @dirname, basename,inode,device,"
                         "permissions,user_owner,group_owner,last_accessed,last_modified,"
                         "last_changed,inode_birth,user_flags,links_to_file,size) "
                         "SET dirname = @dirname, dirname_hash = SHA1(@dirname);").format(path)
  

    try:
        
        print "##################"
        cursor.execute(add_staging_table)
        connection.commit() 
        cursor.callproc('map_staging_table', (media_source_id, os_id))
        cursor.execute("DELETE FROM `staging_table`;")
        connection.commit()
   
    except Exception as err:
        print(err)
        cursor.close()
        return
    
    total_time =  time.time() - start_time
    print "Completed import of {} in ".format(path, total_time)
    cursor.close()

def run(cnx, path):

    if(path == None):
        print "Path is required"
        return

    if(os.path.isfile(path)):
        db_load_file(cnx, path)
#    elif(os.path.isDir(path)):
#        for r, d, f in os.walk(path):
#            while d > 0:
#                d.pop()
#            for file in f:
#                if not file.startswith('.'):
#                    os.path.abspath(os.path.join(r, file))
#                    db_load_file(connection, input_dir + "/" + file)
    else:
        print 'Please input a valid file or a directory for import'
