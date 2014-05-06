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

This module contains core helper functions for Redwood
"""

import sys
import os
import inspect
import time
from collections import namedtuple
from redwood.filters.redwood_filter import RedwoodFilter
from redwood.filters import filter_list
from redwood.foundation.prevalence import PrevalenceAnalyzer

SourceInfo = namedtuple('SourceInfo', 'source_id source_name os_id os_name date_acquired')


def get_filter_by_name(filter_name):
    """
    Fetches an instance of a loaded filter by its name

    :param filter_name: the name of the filter

    :return an instance of a loaded filter with name filter_name
    """
    for f in filter_list:
        if f.name == filter_name:
            return f

    return None

def import_filters(path, cnx):
    """
    Imports filters from an external directory at runtime. Imported filters will be added
    to the global filter_list

    :param path: path where the modules reside
    :param cnx: an instance of the connection

    :return list of newly add filter instances
    """

    new_filters = list()


    print "Importing specified filters from {}".format(path)

    #make sure path exists
    if os.path.isdir(path) is False:
        print "Error: path {} does not exist".format(path)
        return None

    #add the path to the PYTHONPATH
    sys.path.append(path)

    #acquire list of files in the path
    mod_list = os.listdir(path)

    for f in mod_list:

        #continue if it is not a python file
        if f[-3:] != '.py':
            continue

        #get module name by removing extension
        mod_name = os.path.basename(f)[:-3]

        #import the module
        module = __import__(mod_name, locals(), globals())
        for name,cls in inspect.getmembers(module):
            #check name comaprison too since RedwoodFilter is a subclass of itself
            if inspect.isclass(cls) and issubclass(cls, RedwoodFilter) and name != "RedwoodFilter":
                instance = cls()
                #append an instance of the class to the filter_list
                instance.cnx = cnx
                filter_list.append(instance)
                new_filters.append(instance)

    print new_filters

    return new_filters

def get_source_info(cnx, source_name):
    """
    Retrieves a SourceInfo instance given a <source_name>

    :param cnx: a instance of the connection
    :param source_name: name of the media source

    :return SourceInfo instance or None if not found
    """
    cursor = cnx.cursor()

    #query = """
    #    SELECT media_source.id as source_id,
    #           media_source.name as source_name,
    #           os.id as os_id, os.name as os_name,
    #           media_source.date_acquired as date_acquired
    #    FROM media_source
    #    LEFT JOIN os
    #    ON media_source.os_id = os.id
    #    WHERE media_source.name = "{}";""".format(source_name)

    cursor.execute("""
                   SELECT media_source.id as source_id,
                   media_source.name as source_name,
                   os.id as os_id, os.name as os_name,
                   media_source.date_acquired as date_acquired
                   FROM media_source
                   LEFT JOIN os
                   ON media_source.os_id = os.id
                   WHERE media_source.name = %s;""", (source_name,))
    r =  cursor.fetchone()

    if r is None:
        return r

    return SourceInfo(r[0], r[1], r[2],r[3],r[4])

def get_malware_reputation_threshold(cnx):
    """
    Retrieves the max reputation of all confirmed malware

    :param cnx: mysql connection instance

    :return max reputation score
    """

    cursor = cnx.cursor()

    query = """
        select AVG(unique_file.reputation)
            from validator_0 left join unique_file on validator_0.id=unique_file.id
            LEFT JOIN file_metadata ON file_metadata.unique_file_id=unique_file.id where validator_0.status=3;
    """

    cursor.execute(query)

    r = cursor.fetchone()

    if r is None:
        return r

    return r[0]

def get_num_systems(cnx, os_name_or_id):
    """
    Retrieves the number of unique media sources for a given os

    :param cnx: mysql connection instance
    :param os_name_or_id: os name or os id

    :return the number of systems found or None if the os does not exist
    """
    
    cursor = cnx.cursor()
    

    try: 
        val = int(os_name_or_id)

        cursor.execute("""
            SELECT COUNT(media_source.id) FROM os
            LEFT JOIN media_source ON os.id = media_source.os_id
            WHERE os.id = %s
            GROUP BY os.id
            """, (val,))

    except Exception as e:
        cursor.execute("""
            SELECT COUNT(media_source.id) FROM os
            LEFT JOIN media_source ON os.id = media_source.os_id
            WHERE os.id = (SELECT DISTINCT os.id from os where os.name = %s) GROUP BY os.id""", (os_name_or_id,))
    
    r = cursor.fetchone()

    if r is None:
        return None

    return r[0]


def update_analyzers(cnx, sources):
    """
    Runs Analyzers and Filters against each source in the source_os_list, updating the
    approriate tables

    :param sources: list of SourceInfo instances
    """
    print "...Beginning Analyzers and Filters for inputted sources"

    start_time = time.time()

    #now let's run the prevalence analyzer
    pu = PrevalenceAnalyzer(cnx)
    pu.update(sources)

    elapsed_time = time.time() - start_time
    print "...completed analyzers on inputed sources in {}".format(elapsed_time)


def update_filters(cnx, sources):

    start_time = time.time()

    #set the cnx for each plugin
    for p in filter_list:
        p.cnx = cnx

    for source in sources:
        #for source in sources:
        print "==== Beginning filter analysis of {} ====".format(source.source_name)
        for p in filter_list:
            p.update(source.source_name)

    elapsed_time = time.time() - start_time
    print "...completed filter analysis on inputted sources in  {}".format(elapsed_time)



def table_exists(cnx, name):
    """
    Checks if the mysql table with <name> exists

    :param cnx: mysql connection instance
    :param name: table name

    :return True if exists, else False
    """
    cursor = cnx.cursor()
    result = None
    try:
        cursor.execute("""select COUNT(id) from %s""", (name,))
        result = cursor.fetchone()
        cursor.close()
    except Exception as err:
        print err
        pass


    if(result == None or result[0] == 0):
        return False
    else:
        return True

def get_all_sources(cnx):
    """
    Returns a list of all sources currently loaded into Redwood

    :param cnx: mysql connection instance
    """

    cursor = cnx.cursor()
    result = list()
    try:
        cursor.execute("""SELECT media_source.id, media_source.name, os.id, os.name, date_acquired FROM media_source
        INNER JOIN os
        ON media_source.os_id = os.id
        """)
        result = cursor.fetchall()
        cursor.close()
    except Exception as err:
        print err
        return None

    sources = list()
    for r in result:
        sources.append(SourceInfo(r[0],r[1], r[2],r[3],r[4]))

    return sources

def get_reputation_by_source(cnx, source_name):
    """
    Returns a list of scores for every file on the source

    :param cnx: myqsl connection instance
    """

    cursor = cnx.cursor()
    result = list()

    try:
        cursor.execute("""SELECT ROUND(unique_file.reputation, 2),
                       COUNT(DISTINCT unique_file.id) FROM unique_file
                       INNER JOIN file_metadata
                       ON unique_file.id = file_metadata.unique_file_id
                       INNER JOIN media_source
                       ON file_metadata.source_id = media_source.id
                       WHERE media_source.name = %s
                       GROUP BY ROUND(unique_file.reputation, 2)
                       """, (source_name,))
        result = cursor.fetchall()
        cursor.close()
    except Exception as err:
        print err
        return None

    return result
