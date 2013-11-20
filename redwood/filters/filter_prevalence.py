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
This filter provides analysis and scoring based on the prevalence of files and directories

Created on 19 October 2013
@author: Lab41
"""

from redwood.filters.redwood_filter import *
import numpy as np
import matplotlib.pyplot as plt

class FilterPrevalence(RedwoodFilter):
    """
    This FilterPrevalence class uses the occurences of files across systems to 
    assign a reputation score to each unique file
    """

    def __init__(self):
        self.name = "Prevalence"
        self.score_table = "fp_scores"
        self.cnx = None         

    def usage(self):
        
        print "[+] view_by_source <direction> <count> <source> <out_file>"
        print "--- Lists the highest or lowest prevalent files by their average in the specified order"
        print "\t- direction: either \"top\" or \"bottom\""
        print "\t- count: number or results to return from the direction"
        print "\t- source: name of the source"
        print "\t- out_file: file to write results to"
        print "[+] histogram_by_source <source_name>"
        print "---view histogram of file distribution for a single source with name <source_name>"
        print "\t- source_name: name of the source"
        print "[+] histogram_by_os <os_name>"
        print "---view file distribution for an os"
        print "\t- os_name: name of the os"
        print "[+] detect_anomalies <source_name> <out_file>"
        print "---view the top anomalies for the given source"
        print "\t-out_file:  file to write results to"

    def discover_histogram_by_os(self, os_name):
        """
        Displays a histogram of the file distributions across all systems
        of the specified OS

        :param os_name: name of the operating system
        """
       
        #TODO: we can speed this up considerably by doing the counts on the db then
        #just displaying a bar graph.  Currently having the hist function ingest
        #large numbers of points can take way to long

        print '[+] Running \"Histogram by OS\"..."'
        cursor = self.cnx.cursor()
        
        query = """
            SELECT global_file_prevalence.unique_file_id, global_file_prevalence.count 
            FROM redwood.global_file_prevalence LEFT JOIN file_metadata 
            ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id 
            WHERE file_metadata.os_id = (SELECT os.id FROM os where os.name = "{}")
        """.format(os_name)

        cursor.execute(query)

        li = [x[1] for x in cursor.fetchall()]
        cursor.close()
        num_systems = self.get_num_systems(os_name)

        print "NumSystems: {}".format(num_systems)

        bins = range(1, num_systems+2)
        fig = plt.figure()
        ax = fig.add_subplot(111, title="File Prevalence of {}".format(os_name))
        ax.hist(li, color = 'b', bins = bins)
        ax.set_xlabel("Num of Systems")
        ax.set_ylabel("File Occurrences")
    
        plt.show()


    def discover_histogram_by_source(self, source_name):
        """
        Displays a histogram of the file distribution of a single source as it relates
        to all occurrences of that file across all systems

        :param source_name: The name of the souce 
        """

        print '[+] Running \"Histogram by Source\"...'
        cursor = self.cnx.cursor()
       
        src_info = self.get_source_info(source_name)
          
        query = """
            SELECT global_file_prevalence.unique_file_id, global_file_prevalence.count 
            FROM redwood.global_file_prevalence LEFT JOIN file_metadata 
            ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id WHERE file_metadata.source_id = {}
        """.format(src_info.source_id)
        
        cursor.execute(query)
        li = [x[1] for x in cursor.fetchall()]
        cursor.close()
        num_systems = self.get_num_systems(src_info.os_id)
        bins = range(1, num_systems+2)
        fig = plt.figure()
        ax = fig.add_subplot(111, title="File Prevalence of {}".format(src_info.source_name))
        ax.hist(li, color = 'b', bins = bins)
        ax.set_xlabel("Num of Systems")
        ax.set_ylabel("File Occurrences")
    
        plt.show()


    def discover_view_by_source(self, direction, count, source, out):
        """
        Displays avg file prevalence in orderr for a given source

        :param direction: either [top] or [bottom] 
        :param count: number of rows to retrieve from the direction
        :param out: file to write results to 
        """

        print "[+] Running list_by_source..."
        cursor = self.cnx.cursor()
        dir_val = ("desc" if direction is "top" else  "asc")
         
        query = """
            SELECT global_file_prevalence.average, unique_path.full_path, file_metadata.file_name  FROM redwood.global_file_prevalence
            LEFT JOIN file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
            LEFT JOIN unique_path ON file_metadata.unique_path_id = unique_path.id
            WHERE file_metadata.source_id = (SELECT media_source.id FROM media_source WHERE media_source.name = "{}")
            ORDER BY global_file_prevalence.average {} limit 0, {}
        """.format(source, dir_val, count)

        cursor.execute(query)

        with open (out, "w") as f:
            v = 0
            for x in cursor.fetchall():
                f.write("{}: {}   {}{}\n".format(v, x[0], x[1], x[2]))
                v += 1 
        
        cursor.close()
 

    def discover_detect_anomalies(self, source, out):
        """
        Conducts an anomaly search on a given source

        :param source: source
        """
        
        cursor = self.cnx.cursor()

        src_info = self.get_source_info(source)
        
        if src_info is None:
            print "*** Error: Source not found"
            return

        #anomaly type:  low prevalence files in normally high prevalence directories
        print "Anomaly Detection: Unique files in common areas"
        print "running..."
         
        query = """
            SELECT (global_dir_combined_prevalence.average - global_file_prevalence.average) as difference, 
            unique_path.full_path, file_metadata.file_name
            FROM global_file_prevalence 
            LEFT JOIN file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
            LEFT JOIN global_dir_combined_prevalence ON file_metadata.unique_path_id = global_dir_combined_prevalence.unique_path_id
            LEFT JOIN unique_path ON file_metadata.unique_path_id = unique_path.id
            where file_metadata.source_id = {}
            HAVING difference > 0
            ORDER BY difference desc limit 0, 500
        """.format(src_info.source_id)


        cursor.execute(query)

        with open(out, "w") as f:
            v=0
            for x in cursor.fetchall():
                f.write("{}: {}    {}{}".format(v))
                v+=1
             
        cursor.close()


    def update(self, source):
        """
        Updates the scores of the fp_scores table with the new data from the inputted source

        :param source: identifier for the source to be updated
        """

        print "[+] Prevalence Filter running on {} ".format(source)

        #creates the basic tables if they do not exist
        self.build()

        cursor = self.cnx.cursor()

        cursor.execute("SELECT id, os_id from media_source where name = '{}'".format(source))
        r = cursor.fetchone()

        if r is None:
            print "Error: Source with name \"{}\" does not exist".format(source)
            return

        source_id = r[0]
        os_id = r[1]

        
        #initial insert
        query = """
            INSERT INTO  fp_scores(id, score)
            SELECT global_file_prevalence.unique_file_id, IF(num_systems < 3, .5, average) 
            FROM global_file_prevalence JOIN file_metadata
            ON file_metadata.unique_file_id = global_file_prevalence.unique_file_id
            where file_metadata.source_id = {}
            ON DUPLICATE KEY UPDATE score = IF(num_systems < 3, .5, average)
        """.format(source_id)

        cursor.execute(query)
        self.cnx.commit()
        
        #adjustment for low outliers in high prevalent directories... This could probably better be done with taking the std dev of each
        #dir, but his will have to work for now.  
        query = """
            UPDATE  global_file_prevalence left join file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
            LEFT JOIN global_dir_prevalence on file_metadata.unique_path_id = global_dir_prevalence.unique_path_id
            LEFT JOIN global_dir_combined_prevalence on file_metadata.unique_path_id = global_dir_combined_prevalence.unique_path_id 
            LEFT JOIN fp_scores ON fp_scores.id = global_file_prevalence.unique_file_id
            SET fp_scores.score = fp_scores.score * .5 
            where file_metadata.source_id = {} AND global_file_prevalence.count = 1 and global_file_prevalence.num_systems > 2 
            and global_dir_combined_prevalence.average - global_file_prevalence.average > .6
        """.format(source_id)
       
        cursor.execute(query)
        self.cnx.commit()

        #adjustments for low prevalent scored directories which occur often... hopefully this will exclude the caches
        query = """
            UPDATE file_metadata 
            LEFT JOIN global_dir_prevalence ON file_metadata.unique_path_id = global_dir_prevalence.unique_path_id 
            LEFT JOIN global_dir_combined_prevalence ON global_dir_combined_prevalence.unique_path_id = global_dir_prevalence.unique_path_id
            LEFT JOIN fp_scores ON file_metadata.unique_file_id = fp_scores.id
            SET fp_scores.score = (1 - fp_scores.score) * .25 + fp_scores.score
            where file_metadata.source_id = {} AND global_dir_prevalence.average > .8 AND global_dir_combined_prevalence.average < .5
        """.format(source_id)
        
        cursor.execute(query)
        self.cnx.commit()
        cursor.close()

    def clean(self):
        """
        Cleans all tables associated with this filter
        """
        cursor = self.cnx.cursor() 
        cursor.execute("DROP TABLE IF EXISTS fp_scores")
        self.cnx.commit()

    def build(self):
        """
        Builds all persistent tables associated with this filter
        """

        cursor = self.cnx.cursor()
       
        query = """
            CREATE TABLE IF NOT EXISTS `fp_scores` (
            id BIGINT unsigned NOT NULL,
            score double DEFAULT NULL,
            PRIMARY KEY(id),
            CONSTRAINT `fk_unique_file1_id` FOREIGN KEY (`id`) 
            REFERENCES `unique_file` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
                     ) ENGINE=InnoDB
        """
 
        cursor.execute(query)   
        self.cnx.commit()
        cursor.close()
