import sys
import os
import getopt
import string
import time
from datetime import datetime
import MySQLdb
from redwood.foundation.prevalence import PrevalenceAnalyzer
from redwood.filters import plugins


def db_load_file(connection, path):

    try:
        with open(path): pass
    except IOError:
        print 'File \'{}\' does not exist'.format(path)
        return
    
    start_time = time.time()
    print "...Loading file \"{}\"".format(path)

    filename = os.path.basename(path)
    fields = string.split(filename, '--')

    if(len(fields) != 3):
        print "Error: Improper naming scheme"
        return
    cursor = connection.cursor()
    os_id = None

    source_name = fields[2]

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
        date_object = datetime.strptime(fields[0], '%Y-%m-%d')

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
        source_id = cursor.lastrowid

    except MySQLdb.Error, e:
        if connection:
            connection.rollback()                       
            print "Error %d: %s" % (e.args[0],e.args[1])
            return                                        

    media_source_id = cursor.lastrowid
    

    path = path.replace('\\','\\\\')
    #load raw csv into the staging table from the client
    add_staging_table = ("LOAD DATA LOCAL INFILE '{}' INTO TABLE `staging_table` "
                         "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' "
                         "IGNORE 1 LINES "
                         "(global_file_id, parent_id, dirname, basename,contents_hash,dirname_hash,filesystem_id,device_id,"
                         "attributes,user_owner,group_owner,size,@created_param,@accessed_param,@modified_param,@changed_param,"
                         "@user_flags,links_to_file, @disk_offset, @entropy, @file_content_status, @extension, file_type) "
                         "SET created = FROM_UNIXTIME(@created_param), last_accessed = FROM_UNIXTIME(@accessed_param),"
                         "last_modified = FROM_UNIXTIME(@modified_param), last_changed = FROM_UNIXTIME(@changed_param),"
                         "user_flags = nullif(@user_flags,''), disk_offset = nullif(@disk_offset,''),"
                         "entropy=nullif(@entropy,''), file_content_status=nullif(@file_content_status,''),"
                         "extension = nullif(@extension,'');").format(path) 


    try:
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
    print "\t[*] completed in {}".format(total_time)
    cursor.close()
    
    return (source_id, source_name, os_id)

def run(cnx, path):

    
    src_os_list = list()

    if(path == None):
        print "Path is required"
        return
    
    if(os.path.isfile(path)):
        r =  db_load_file(cnx, path)
        src_os_list.append(r)
    elif(os.path.isdir(path)):
        for r, d, f in os.walk(path):
            while len(d) > 0:
                d.pop()
            for file in f:
                if not file.startswith('.'):
                    os.path.abspath(os.path.join(r, file))
                    r = db_load_file(cnx, path + "/" + file)
                    src_os_list.append(r)
    else:
        print 'Please input a valid file or a directory for import'
        return

    
    print "source os"
    print src_os_list

    #now let's run the prevalence analyzer
    pu = PrevalenceAnalyzer(cnx)
    pu.update(src_os_list)

    #set the cnx for each plugin
    for p in plugins:
        p.cnx = cnx

    print "[*] Beginning filter analysis"
    for src_id, source_name, os_id in src_os_list:
        print "...source {}".format(src_id)
        for p in plugins:
            print "......filter {}".format(p.name)
            p.update(source_name)
