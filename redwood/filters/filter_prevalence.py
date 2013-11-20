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
@author: Paul
"""

from redwood.filters.redwood_filter import *
import numpy as np
import matplotlib.pyplot as plt

class FilterPrevalence(RedwoodFilter):

    """
    This FilterPrevalence class provides the "Prevalence" filter functionality
    """

    def __init__(self):
        self.name = "Prevalence"
        self.score_table = "fp_scores"
        self.cnx = None         

    def usage(self):
        
        print "view_high <count>"
        print "\t-displays top <count> scores for this filter"
        print "view_low <count>"
        print "\t-displays lowest <count> score for this filter"
        print "histogram_by_source <source_name>"
        print "\t-view file distribution for a single source with name <source_name>"
        print "histogram_by_os <os_name>"
        print "\t-view file distribution for an os"


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


    def discover_directory_prevalence(self, count, direction):
        """
        Shows the top or bottom <count> directories based on prevalence analysis

        :param count: the number to display from the top or bottom
        :param direction: either \"top\" or \"bottom\"
        """

        cursor = self.cnx.cursor()

        if refresh is 'true':
            query = """
                INSERT into dir_prevalence(unique_path_id, avg_score, count)
                    SELECT unique_path_id, average_score, t2.cnt from (SELECT unique_path_id, avg(average) as average_score from 
                        (SELECT avg(score) as average, unique_path_id  FROM file_metadata 
                            INNER JOIN filter_prevalence ON file_metadata.unique_file_id = filter_prevalence.unique_file_id 
                            GROUP BY unique_path_id) as t 
                        GROUP BY unique_path_id) as t0 inner join
                        (SELECT COUNT(id) as cnt, unique_path_id as path_id 
                    FROM file_metadata where file_name = '/' GROUP BY unique_path_id) as t2 on unique_path_id = t2.path_id
            """

            cursor.execute(query)


        if direction is 'high':
            pass
        elif direction is 'low':
            pass
        else:
            print "Direction must be \"high\" or \"low\""
            return



    def discover_view_high(self, count):
        if(self.table_exists() == False):
            self.run()
         
        cursor = self.cnx.cursor()
        
        query = ("select full_path, file_name, score, file_type " 
                "from joined_file_metadata JOIN filter_prevalence "
                "ON (joined_file_metadata.unique_file_id = filter_prevalence.unique_file_id) "
                "where (joined_file_metadata.file_type != '_dir') "
                "ORDER BY score DESC "
                "limit 0 , {}").format(count)

        cursor.execute(query)
        v = 0
        for x in cursor.fetchall():
            print "{}:{} ({}) {}{}".format(v, x[2], x[3], x[0], x[1])
            v += 1 
        
        cursor.close()
        
        

    def discover_view_low(self, count):
        
        if(self.table_exists() == False):
            self.run()
         
        cursor = self.cnx.cursor()
        
        query = ("select full_path, file_name, score, file_type " 
                "from joined_file_metadata JOIN filter_prevalence "
                "ON (joined_file_metadata.unique_file_id = filter_prevalence.unique_file_id) "
                "where (joined_file_metadata.file_type != '_dir') "
                "ORDER BY score ASC "
                "limit 0 , {}").format(count)

        cursor.execute(query)
       
        v = 0
        for x in cursor.fetchall():
            print "{}:{} ({}) {}{}".format(v, x[2], x[3], x[0], x[1])
            v += 1 
        cursor.close()
        

    def run(self):

        self.clean()
        self.build()
        cursor = self.cnx.cursor()
        
        cursor.execute(query)

        #initialize the fp_scores table. The score is the average unless we don't have enough systems
        #which is less than 3 for now, in which case we just set the score to .5
        query = """
            INSERT INTO  fp_scores(id, score)
            SELECT unique_file_id, IF(num_systems < 3, .5, average) FROM global_file_prevalence
        """
        
        cursor.execute(query)
        self.cnx.commit()
        

        #adjustment for low outliers in high prevalent directories... This could probably better be done with taking the std dev of each
        #dir, but his will have to work for now. beware duplication here... TODO
        query = """
            UPDATE  global_file_prevalence left join file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
            LEFT JOIN global_dir_prevalence on file_metadata.unique_path_id = global_dir_prevalence.unique_path_id
            LEFT JOIN global_dir_combined_prevalence on file_metadata.unique_path_id = global_dir_combined_prevalence.unique_path_id 
            LEFT JOIN fp_scores ON fp_scores.id = global_file_prevalence.unique_file_id
            SET fp_scores.score = fp_scores.score * .5 
            where global_file_prevalence.num_systems > 2 and global_dir_combined_prevalence.average - global_file_prevalence.average > .6
        """
       
        cursor.execute(query)
        self.cnx.commit()

        #adjustments for low prevalent scored directories which occur often... hopefully this will exclude the caches
        query = """
            UPDATE file_metadata 
            LEFT JOIN global_dir_prevalence ON file_metadata.unique_path_id = global_dir_prevalence.unique_path_id 
            LEFT JOIN global_dir_combined_prevalence ON global_dir_combined_prevalence.unique_path_id = global_dir_prevalence.unique_path_id
            LEFT JOIN fp_scores ON file_metadata.unique_file_id = fp_scores.id
            SET fp_scores.score = (1 - fp_scores.score) * .25 + fp_scores.score
            where global_dir_prevalence.average > .8 AND global_dir_combined_prevalence.average < .5
        """
        
        cursor.exceute(query)
        self.cnx.commit()
        cursor.close()

    def update(self, source):
 
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
         
        cursor = self.cnx.cursor() 
        cursor.execute("DROP TABLE IF EXISTS fp_scores")
        self.cnx.commit()

    def build(self):
 
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
