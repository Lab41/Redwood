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
from redwood.filters import RedwoodFilter
from redwood.filters import filter_list
from redwood.foundation.prevalence import PrevalenceAnalyzer

SourceInfo = namedtuple('SourceInfo', 'source_id source_name os_id os_name')

def import_filters(path):
    """
    Imports filters from an external directory at runtime. Imported filters will be added
    to the global filter_list

    :param path: path where the modules reside
    :return list of newly add filter instances
    """

    new_filters = list()

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
                filter_list.append(instance)
                new_filters.append(instance)

    return new_filters

def get_source_info(cnx, source_name):
    """
    Retrieves a SourceInfo instance given a <source_name>

    :param source_name: name of the media source

    :return SourceInfo instance or None if not found
    """
    cursor = cnx.cursor()
    
    query = """
        SELECT media_source.id as source_id, media_source.name as source_name, os.id as os_id, os.name as os_name
        FROM media_source LEFT JOIN os ON media_source.os_id = os.id where media_source.name = "{}";
    """.format(source_name)
   
    cursor.execute(query)
    r =  cursor.fetchone()
    
    if r is None:
        return r

    return SourceInfo(r[0], r[1], r[2],r[3])

def get_num_systems(cnx, os_name_or_id):
    """
    Retrieves the number of unique media sources for a given os
    
    :param cnx: mysql connection instance
    :param os_name_or_id: os name or os id

    :return the number of systems found or None if the os does not exist
    """
    if isinstance(os_name_or_id, (int, long, float, complex)):
        os_id = os_name_or_id
    else:
        os_id = "(SELECT DISTINCT os.id from os where os.name = \"{}\")".format(os_name_or_id)

    cursor = cnx.cursor()
    
    query = """
        SELECT COUNT(media_source.id) FROM os 
        LEFT JOIN media_source ON os.id = media_source.os_id
        WHERE os.id = {}
        GROUP BY os.id
    """.format(os_id)
    
    cursor.execute(query)
    r = cursor.fetchone()
    if r is None:
        return None
    
    return r[0]


def update_analyzers_and_filters(cnx, sources):
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

    #set the cnx for each plugin
    for p in filter_list:
        p.cnx = cnx

    for source in sources:
        print "==== Beginning filter analysis of {} ====".format(source.source_name)
        for p in filter_list:
            p.update(source.source_name)

    elapsed_time = time.time() - start_time
    print "...completed Analyzers and Filters in {}".format(elapsed_time) 



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
        cursor.execute("select COUNT(id) from {}".format(name))
        result = cursor.fetchone()
        cursor.close()
    except Exception as err:
        pass

   
    if(result == None or result[0] == 0):
        return False
    else: 
        return True


