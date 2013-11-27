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
from collections import namedtuple


class RedwoodFilter(object):
    """
    Base class for Filter creation

    :ivar name: Name of the filter
    :ivar cnx: connection instance to the database
    :ivar score_table: name of the table containing reputation scores. The table must have exactly two columns (id, score) 
    """
    def __init__(self):
        self.name = "generic"
        self.cnx = None    
        self.score_table = None
    def clean(self):
        """
        Deletes all required tables for this filter (method must be overridden)
        """
        pass
    def update(self, source):
        """
        Updates filter tables with new data from <source>  (method must be overridden)

        :param source: name of the media source
        """
        pass
    def rebuild(self):
        """
        Deletes all tables for this filter, recreates them, then rebuilds data for them from the datastore
        """
        self.clean()
        self.build()

        #get a list of the sources
        query = """
            SELECT media_source.name FROM media_source
        """

        cursor = self.cnx.cursor()
        cursor.execute(query)
        
        print "...Rebuild process started"
        for source in cursor.fetchall():
            print "rebuilding for source: {}".format(source[0])
            self.update(source[0])
        
    def show_results(self, direction, count, source, out):
        """
        Displays avg file prevalence in orderr for a given source

        :param direction: either [top] or [bottom] 
        :param count: number of rows to retrieve from the direction
        :param out: file to write results to 
        """

        print "[+] Running list_by_source..."
        cursor = self.cnx.cursor()
        dir_val = ("desc" if direction == "top" else  "asc")

        query = """
            SELECT {}.score, unique_path.full_path, file_metadata.file_name 
            FROM {} LEFT JOIN file_metadata ON {}.id = file_metadata.unique_file_id
            LEFT JOIN unique_path ON file_metadata.unique_path_id = unique_path.id
            WHERE file_metadata.source_id = (SELECT media_source.id FROM media_source WHERE media_source.name = "{}")
            ORDER BY {}.score {} LIMIT 0, {}
        """.format(self.score_table, self.score_table, self.score_table, source, self.score_table, dir_val, count)

        cursor.execute(query)

        with open (out, "w") as f:
            v = 0
            for x in cursor.fetchall():
                f.write("{}: {}   {}{}\n".format(v, x[0], x[1], x[2]))
                v += 1 
        
        cursor.close()
 
    def run_func(self, func_name, args):
        """
        Helper function that will run the <func_name> with <args> for this filter

        :param func_name: name of the function to run
        :param args: list of arguments to run with the function
        """
        f = self.__getattribute__(func_name)
        f(*args)
