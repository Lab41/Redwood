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
import inspect

class RedwoodFilter(object):
    """
    Base class for Filter creation

    :ivar name: Name of the filter. This should be one word, lower case, with underscores if needed
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
        raise NotImplementedError

    def update(self, source):
        """
        Updates filter tables with new data from <source>  (method must be overridden)

        :param source: name of the media source
        """
        raise NotImplementedError

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

    def show_results(self, direction, count, source, out=None):
        """
        Displays avg file prevalence in orderr for a given source

        :param direction: either [top] or [bottom]
        :param count: number of rows to retrieve from the direction
        :param out: file to write results to
        """

        print "[+] Running list_by_source..."
        cursor = self.cnx.cursor()
        dir_val = ("desc" if direction == "top" else  "asc")

        if direction == "top":
            dir_val = "desc"
        elif direction == "bottom":
            dir_val = "asc"
        else:
            print "Error:  direction must be \"top\" or \"bottom\""
            return


        print "Fetching {} results from {} for filter {}".format(direction, source, self.name)

        query = """
            SELECT {}.score, unique_path.full_path, file_metadata.file_name
            FROM {} LEFT JOIN file_metadata ON {}.id = file_metadata.unique_file_id
            LEFT JOIN unique_path ON file_metadata.unique_path_id = unique_path.id
            WHERE file_metadata.source_id = (SELECT media_source.id FROM media_source WHERE media_source.name = "{}")
            ORDER BY {}.score {} LIMIT 0, {}
        """.format(self.score_table, self.score_table, self.score_table, source, self.score_table, dir_val, count)

        cursor.execute(query)

        if out is None:
            results = cursor.fetchall()
            i = 0
            for r in results:
                print "{}:\t{}\t{}/{}".format(i, r[0], r[1], r[2])
                i+=1
            return results
        else:

            with open (out, "w") as f:
                v = 0
                for x in cursor.fetchall():
                    f.write("{}:\t{}\t{}/{}\n".format(v, x[0], x[1], x[2]))
                    v += 1

        cursor.close()


    def build(self):
        """
        Builds necessary tables for the filter. This function must create the scores table. The standard practice
        is to create a table called "filter_name"_scores that has two columns (id, double score). As an example for a
        filter called "woohoo", you would want to add the following create table::

            CREATE TABLE IF NOT EXISTS `woohoo_scores` (
                id BIGINT unsigned NOT NULL,
                score double DEFAULT NULL,
                PRIMARY KEY(id),
                CONSTRAINT `fk_unique_file_woohoo_id` FOREIGN KEY (`id`)
                REFERENCES `unique_file` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
                ) ENGINE=InnoDB
        """

        raise NotImplementedError

    def run_survey(self, source_name):
        """
        Given a source name, this function will create an html file summarizing its analysis. The survey should be an
        html file named "survey.html", and it should be located in a directory called "survey_[your file name]_[source name].
        The survey directory should also contain a resources directory where html resources such as images will be saved::

            survey_filtername__sourcename
            |- survey.html
            |- resources

        :param source_name: name of the source

        :return path to the survey directory
        """

        raise NotImplementedError

    def run_func(self, func_name, *args):
        """
        Helper function that will run the <func_name> with <args> for this filter

        :param func_name: name of the function to run
        :param args: list of arguments to run with the function
        """
        func = getattr(self, 'discover_' + func_name, None)
        if not func:
            return False

        ret = inspect.getargspec(func)
        #subtract one for the "self"
        upper_num_args = len(ret.args) - 1

        if ret.defaults is not None:
            lower_num_args = upper_num_args - len(ret.defaults)
        else:
            lower_num_args = upper_num_args

        actual_args = len(args)

        if actual_args > upper_num_args or actual_args < lower_num_args:
            print "Error: Incorrect number of args"
            return False

        func(*args)
        return True

    def do_help(self, cmd):
        "Get help on a command. Usage: help command"
        if cmd: 
            func = getattr(self, 'discover_' + cmd, None)
            if func:
                print func.__doc__
                return True
        return False
