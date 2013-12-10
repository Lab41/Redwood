#!/usr/bin/env python
#
# Copyright (c) 2013 In-Q-Tel, Inc/Lab41, All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Created on 19 October 2013
@author: Lab41
"""


import sys
import os
import shutil
import getopt
import string
import time
from datetime import datetime
import MySQLdb
from redwood.helpers.core import SourceInfo
from redwood.foundation.prevalence import PrevalenceAnalyzer
from redwood.filters import filter_list
import redwood.helpers.core as core
from redwood.foundation.report import Report

def db_load_file(connection, path):
    """
    Loads a file located at <path> into the database

    :param connection: connection object for the database
    :param path: path where the file is located
    
    :return SourceInfo representing the inputted source
    """
    
    try:
        with open(path): pass
    except IOError:
        print '*** Error: File \'{}\' does not exist'.format(path)
        return
    

    filename = os.path.basename(path)
    fields = string.split(filename, '--')

    if(len(fields) != 3):
        print "*** Error: Improper naming scheme"
        return
    cursor = connection.cursor()
    os_id = None

    source_name = fields[2]
    os_name = fields[1]

    print "=== Loading \"{}\" into database ===".format(source_name)
    #transaction for adding to media and os tables. Both succeed or both fail
    try:

        data_os = {
            'name':os_name,
        }

        #add os 
        add_os = ("INSERT INTO `os` (name) VALUES('%(name)s') ON DUPLICATE KEY UPDATE id=id") % data_os
        cursor.execute(add_os)
        connection.commit()
        
    except MySQLdb.Error, e:
        if connection:
            connection.rollback()                       
            print "*** Error %d: %s" % (e.args[0],e.args[1])
            return                                        

    #now get the os_id for the os_name
    query = "SELECT os.id FROM os WHERE os.name = \"{}\"".format(os_name)
    cursor.execute(query)
    r = cursor.fetchone()
    os_id = r[0]

    if(os_id is None):
        print "*** Error: Unable to find corresponding os"
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
            print "*** Error %d: %s" % (e.args[0],e.args[1])
            sys.exit(1)                                        

    media_source_id = cursor.lastrowid
    
    path = path.replace('\\','\\\\')
    #load raw csv into the staging table from the client
    add_staging_table = ("LOAD DATA LOCAL INFILE '{}' INTO TABLE `staging_table` "
                         "FIELDS TERMINATED BY ','  ENCLOSED BY '\"' LINES TERMINATED BY '\\n' "
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

        #create the staging table
        query = """
            CREATE TABLE IF NOT EXISTS staging_table (
            global_file_id LONG NOT NULL,
            parent_id LONG NULL,
            dirname VARCHAR(4096) NULL,
            basename VARCHAR(255) NULL,
            contents_hash CHAR(40) NULL,
            dirname_hash CHAR(40) NULL,
            filesystem_id INT UNSIGNED NULL,
            device_id INT NULL,
            attributes INT NULL,
            user_owner INT NULL,
            group_owner INT NULL,
            size INT UNSIGNED NULL,
            created DATETIME NULL,
            last_accessed DATETIME NULL,
            last_modified DATETIME NULL,
            last_changed DATETIME NULL,
            user_flags INT NULL DEFAULT NULL,
            links_to_file INT NULL,
            disk_offset BIGINT  NULL,
            entropy TINYINT  NULL,
            file_content_status TINYINT NULL,
            extension VARCHAR(32)  NULL,
            file_type VARCHAR(64)  NULL,
            INDEX contents_hash_idx (contents_hash ASC),
            INDEX dirname_hash_idx (dirname_hash ASC)
            )  ENGINE=InnoDB;
        """
        
        cursor.execute(query)
        connection.commit()

        start_time = time.time()
        cursor.execute(add_staging_table)
        connection.commit() 
        print "...data transfer to staging table in {}".format(time.time() - start_time)
        start_time = time.time()
        
        cursor.callproc('map_staging_table', (media_source_id, os_id))
        cursor.execute("DROP TABLE `staging_table`;")
        connection.commit()
        print "...data written from staging table to main tables in {}".format(time.time() - start_time)
    except Exception as err:
        print "Exception occurred: {}".format(err)
        cursor.close()
        sys.exit(1)
    
    total_time =  time.time() - start_time
    print "...completed in {}".format(total_time)
    cursor.close()
    
    return SourceInfo(source_id, source_name, os_id, os_name) 

def run(cnx, path):
    """
    Loads all csv files from the path into the database

    :param cnx: mysql connection object
    :param path: directory containing csv files or the full path to a csv file
    """
    src_os_list = list()

    if(path == None):
        print "*** Error: Path is required"
        return
    
    if(os.path.isfile(path)):
        info =  db_load_file(cnx, path)
        src_os_list.append(info)
    elif(os.path.isdir(path)):
        for r, d, f in os.walk(path):
            while len(d) > 0:
                d.pop()
            for file in f:
                if not file.startswith('.'):
                    os.path.abspath(os.path.join(r, file))
                    info = db_load_file(cnx, path + "/" + file)
                    src_os_list.append(info)
    else:
        print 'Please input a valid file or a directory for import'
        return

    #update the analyzers and filters
    core.update_analyzers_and_filters(cnx,src_os_list)
    report_dir = "reports"
    rpt = Report(cnx)
    
    for f in filter_list:
        for src in src_os_list:
            path = f.run_survey(src[1])
            print "PATH: " + path
            shutil.rmtree(report_dir + "/" + src[1] + "/filters/" + f.name)
            shutil.move(path, report_dir + "/" + src[1] + "/filters/" + f.name)
            
            print "NEW PATH: " + report_dir + "/" + src[1] + "/filters/" + f.name
            rpt.generate_report(src)
