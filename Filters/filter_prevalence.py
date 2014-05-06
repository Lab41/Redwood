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

import os
import numpy as np
import matplotlib.pyplot as plt
import redwood.helpers.core as core
import shutil

from redwood.filters.redwood_filter import RedwoodFilter

class FilterPrevalence(RedwoodFilter):
    """
    This filter provides analysis and scoring based on the prevalence of files and directories across sources. The general idea is that a file with a higher prevalence would have a higher reputation than a file that occurs less often.
    """

    def __init__(self, cnx=None):
        self.name = "Prevalence"
        self.score_table = "fp_scores"
        self.cnx = cnx

    def usage(self):
        """
        Prints the usage statement
        """

        print "[+] histogram_by_source <source_name>"
        print "---view histogram of file distribution for a single source with name <source_name>"
        print "\t- source_name: name of the source"
        print "[+] histogram_by_os <os_name>"
        print "---view file distribution for an os"
        print "\t- os_name: name of the os"
        print "[+] detect_anomalies <source_name> <out_file>"
        print "---view the top anomalies for the given source"
        print "\t-out_file:  file to write results to"


    def update(self, source):
        """
        Updates the scores of the fp_scores table with the new data from the inputted source

        :param source: identifier for the source to be updated
        """

        print "[+] Prevalence Filter running on {} ".format(source)

        #creates the basic tables if they do not exist
        self.build()

        cursor = self.cnx.cursor()

        src_info = core.get_source_info(self.cnx, source)

        if src_info is None:
            print "Error: Source {} not found".format(source)
            return

        #initial insert
        #query = """
        #    INSERT INTO  fp_scores(id, score)
        #    SELECT global_file_prevalence.unique_file_id, IF(num_systems < 3, .5, average)
        #    FROM global_file_prevalence JOIN file_metadata
        #    ON file_metadata.unique_file_id = global_file_prevalence.unique_file_id
        #    where file_metadata.source_id = {}
        #    ON DUPLICATE KEY UPDATE score = IF(num_systems < 3, .5, average)
        #""".format(src_info.source_id)

        cursor.execute("""
                       INSERT INTO  fp_scores(id, score)
                       SELECT global_file_prevalence.unique_file_id,
                       IF(num_systems < 3, .5, average)
                       FROM global_file_prevalence JOIN file_metadata
                       ON file_metadata.unique_file_id = global_file_prevalence.unique_file_id
                       where file_metadata.source_id = %s
                       ON DUPLICATE KEY UPDATE score =
                       IF(num_systems < 3, .5, average)
                       """, (src_info.source_id,))
        self.cnx.commit()

        #adjustment for low outliers in high prevalent directories... This could probably better be done with taking the std dev of each
        #dir, but his will have to work for now.  
        #query = """
        #    UPDATE  global_file_prevalence left join file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
        #    LEFT JOIN global_dir_prevalence on file_metadata.unique_path_id = global_dir_prevalence.unique_path_id
        #    LEFT JOIN global_dir_combined_prevalence on file_metadata.unique_path_id = global_dir_combined_prevalence.unique_path_id
        #    LEFT JOIN fp_scores ON fp_scores.id = global_file_prevalence.unique_file_id
        #    SET fp_scores.score = fp_scores.score * .5
        #    where file_metadata.source_id = {} AND global_file_prevalence.count = 1 and global_file_prevalence.num_systems > 2
        #    and global_dir_combined_prevalence.average - global_file_prevalence.average > .6
        #""".format(src_info.source_id)

        cursor.execute("""
                       UPDATE  global_file_prevalence left join file_metadata
                       ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
                       LEFT JOIN global_dir_prevalence
                       on file_metadata.unique_path_id = global_dir_prevalence.unique_path_id
                       LEFT JOIN global_dir_combined_prevalence
                       on file_metadata.unique_path_id = global_dir_combined_prevalence.unique_path_id
                       LEFT JOIN fp_scores
                       ON fp_scores.id = global_file_prevalence.unique_file_id
                       SET fp_scores.score = fp_scores.score * .5
                       where file_metadata.source_id = %s
                       AND global_file_prevalence.count = 1
                       and global_file_prevalence.num_systems > 2
                       and global_dir_combined_prevalence.average - global_file_prevalence.average > .6
                       """, (src_info.source_id,))
        self.cnx.commit()

        #adjustments for low prevalent scored directories which occur often... hopefully this will exclude the caches
        #query = """
        #    UPDATE file_metadata
        #    LEFT JOIN global_dir_prevalence ON file_metadata.unique_path_id = global_dir_prevalence.unique_path_id
        #    LEFT JOIN global_dir_combined_prevalence ON global_dir_combined_prevalence.unique_path_id = global_dir_prevalence.unique_path_id
        #    LEFT JOIN fp_scores ON file_metadata.unique_file_id = fp_scores.id
        #    SET fp_scores.score = (1 - fp_scores.score) * .25 + fp_scores.score
        #    where file_metadata.source_id = {} AND global_dir_prevalence.average > .8 AND global_dir_combined_prevalence.average < .5
        #""".format(src_info.source_id)

        cursor.execute("""
                       UPDATE file_metadata
                       LEFT JOIN global_dir_prevalence
                       ON file_metadata.unique_path_id = global_dir_prevalence.unique_path_id
                       LEFT JOIN global_dir_combined_prevalence
                       ON global_dir_combined_prevalence.unique_path_id = global_dir_prevalence.unique_path_id
                       LEFT JOIN fp_scores
                       ON file_metadata.unique_file_id = fp_scores.id
                       SET fp_scores.score = (1 - fp_scores.score) * .25 + fp_scores.score
                       where file_metadata.source_id = %s
                       AND global_dir_prevalence.average > .8
                       AND global_dir_combined_prevalence.average < .5
                       """, (src_info.source_id,))
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
            id BIGINT UNSIGNED NOT NULL,
            score double DEFAULT NULL,
            PRIMARY KEY(id),
            CONSTRAINT `fk_unique_file1_id` FOREIGN KEY (`id`)
            REFERENCES `unique_file` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
                     ) ENGINE=InnoDB
        """

        cursor.execute(query)
        self.cnx.commit()
        cursor.close()



    ##################################################
    #
    #       DISCOVERY FUNCTIONS
    #
    ##################################################

    def discover_histogram_by_os(self, os_name, output=None):
        """
        Displays a histogram of the file distributions across all systems
        of the specified OS

        :param os_name: name of the operating system
        :param output: (optional) output filename in PNG format
        """

        print '[+] Running \"Histogram by OS\"..."'
        cursor = self.cnx.cursor()
        
        num_systems = core.get_num_systems(self.cnx, os_name)

        print "NUM: {}".format(num_systems)
        if num_systems is None or num_systems == 0:
            print "Error: OS {} does not exist".format(os_name)
            return

        bins = range(1, num_systems+2)

        #query = """
        #    SELECT COUNT(file_metadata.os_id), global_file_prevalence.count FROM global_file_prevalence
        #    LEFT JOIN file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
        #    WHERE file_metadata.os_id = (SELECT os.id FROM os WHERE os.name = "{}")
        #    GROUP BY global_file_prevalence.count ORDER BY global_file_prevalence.count ASC;
        #""".format(os_name)

        cursor.execute("""
                       SELECT COUNT(file_metadata.os_id), global_file_prevalence.count
                       FROM global_file_prevalence
                       LEFT JOIN file_metadata
                       ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
                       WHERE file_metadata.os_id =
                       (SELECT os.id FROM os WHERE os.name = %s)
                       GROUP BY global_file_prevalence.count
                       ORDER BY global_file_prevalence.count ASC;
                       """, (os_name,))
        data = cursor.fetchall()
        counts, ranges = zip(*data)

        fig = plt.figure()
        perc = int( float(sum(counts[1:])) / sum(counts) * 100)
        ax = fig.add_subplot(111, title="File Prevalence of {} with {}% > 1".format(os_name, perc))
        ax.hist(ranges, weights=counts, bins = bins)
        ax.set_xlabel("Num of Systems")
        ax.set_ylabel("File Occurrences")

        if output is None:
            plt.show()
        else:
            print "Saving histogram to {}".format(output)
            plt.savefig(output)

    def discover_histogram_by_source(self, source_name, output=None):
        """
        Displays a histogram of the file distribution of a single source as it relates
        to all occurrences of that file across all systems

        :param source_name: The name of the source
        :param output: (optional) output filename in PNG format
        """

        print '[+] Running \"Histogram by Source\"...'
        cursor = self.cnx.cursor()

        src_info = core.get_source_info(self.cnx, source_name)

        if src_info is None:
            print "Source {} does not exist".format(source_name)
            return

        num_systems = core.get_num_systems(self.cnx, src_info.os_id)
        bins = range(1, num_systems+2)

        #query = """
        #    SELECT COUNT(file_metadata.id), global_file_prevalence.count FROM global_file_prevalence
        #    LEFT JOIN file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
        #    WHERE file_metadata.source_id = (SELECT media_source.id FROM media_source WHERE media_source.name = "{}")
        #    GROUP BY global_file_prevalence.count ORDER BY global_file_prevalence.count ASC;
        #""".format(source_name)

        cursor.execute("""
                       SELECT COUNT(file_metadata.id), global_file_prevalence.count
                       FROM global_file_prevalence
                       LEFT JOIN file_metadata
                       ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
                       WHERE file_metadata.source_id =
                       (SELECT media_source.id
                        FROM media_source
                        WHERE media_source.name = %s)
                       GROUP BY global_file_prevalence.count
                       ORDER BY global_file_prevalence.count ASC;
                       """, (source_name,))

        data = cursor.fetchall()

        if data == None or len(data) is 0:
            return

        counts, ranges = zip(*data)

        fig = plt.figure()
        perc = int( float(sum(counts[1:])) / sum(counts) * 100)
        ax = fig.add_subplot(111, title="File Prevalence of {} with {}% > 1".format(src_info.source_name, perc))
        ax.hist(ranges, weights=counts, bins = bins)
        ax.set_xlabel("Num of Systems")
        ax.set_ylabel("File Occurrences")

        if output is None:
            plt.show()
        else:
            print "Saving histogram to {}".format(output)
            plt.savefig(output)

    def discover_detect_anomalies(self, source, out=None):
        """
        Conducts an anomaly search on a given source

        :param source: source
        :param out: output file (optional)
        """

        cursor = self.cnx.cursor()

        src_info = core.get_source_info(self.cnx, source)

        if src_info is None:
            print "*** Error: Source not found"
            return

        #anomaly type:  low prevalence files in normally high prevalence directories
        print "...Anomaly Detection: Unique files in common areas"

        #query = """
        #    SELECT (global_dir_combined_prevalence.average - global_file_prevalence.average) as difference,
        #    unique_path.full_path, file_metadata.file_name
        #    FROM global_file_prevalence
        #    LEFT JOIN file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
        #    LEFT JOIN global_dir_combined_prevalence ON file_metadata.unique_path_id = global_dir_combined_prevalence.unique_path_id
        #    LEFT JOIN unique_path ON file_metadata.unique_path_id = unique_path.id
        #    where file_metadata.source_id = {}
        #    HAVING difference > 0
        #    ORDER BY difference desc limit 0, 100
        #""".format(src_info.source_id)

        cursor.execute("""
                       SELECT (global_dir_combined_prevalence.average - global_file_prevalence.average) as difference,
                       unique_path.full_path, file_metadata.file_name
                       FROM global_file_prevalence
                       LEFT JOIN file_metadata
                       ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
                       LEFT JOIN global_dir_combined_prevalence ON file_metadata.unique_path_id = global_dir_combined_prevalence.unique_path_id
                       LEFT JOIN unique_path
                       ON file_metadata.unique_path_id = unique_path.id
                       where file_metadata.source_id = %s
                       HAVING difference > 0
                       ORDER BY difference desc limit 0, 100
                       """, (src_info.source_id,))

        if out is None:
            results = cursor.fetchall()
            if results is None or len(results) == 0:
                print "No anomalies found"
            else:
                print "Showing top {} results".format(len(results))
                for x in results:
                    print x
            return results

        print "Writing results to {}".format(out)

        with open(out, "w") as f:
            v=0
            for x in cursor.fetchall():
                f.write("{}: {}    {}/{}\n".format(v, x[0], x[1], x[2]))
                v+=1

        cursor.close()

    def run_survey(self, source_name):

        print "...running survey for {}".format(self.name)

        resources = "resources"
        img_by_src = "hist_by_src.png"
        img_by_os = "hist_by_os.png"
        survey_file = "survey.html"
        survey_dir = "survey_{}_{}".format(self.name, source_name)


        resource_dir = os.path.join(survey_dir, resources) 
        html_file = os.path.join(survey_dir, survey_file)

        try:
            shutil.rmtree(survey_dir)
        except:
            pass

        os.mkdir(survey_dir)
        os.mkdir(resource_dir)

        src_info = core.get_source_info(self.cnx, source_name)

        self.discover_histogram_by_source(source_name, os.path.join(resource_dir, img_by_src))
        self.discover_histogram_by_os(src_info.os_name, os.path.join(resource_dir, img_by_os))
        anomalies = self.discover_detect_anomalies(source_name, None)
        results = self.show_results("bottom", 100, source_name, None)


        with open(html_file, 'w') as f:
            f.write("""
            <html>
            <link href="../../../resources/css/style.css" rel="stylesheet" type="text/css">
            <h2 class="redwood-title">Filter Prevalence Snapshot</h2>
            <body>
                <h3 class="redwood-header">Histogram for {}</h3>
                <img src="{}">
                <h3 class="redwood-header">Histogram for Operating System - {}</h3>
                <img src="{}">
            """.format( source_name,
                        os.path.join(resources, img_by_src),
                        src_info.os_name,
                        os.path.join(resources, img_by_os)
                        ))

            f.write("<h3 class=\"redwood-header\">The lowest 100 reputations for this filter</h3>")
            f.write("<table border=\"1\" id=\"redwood-table\">")
            f.write("<thead><tr><th class=\"rounded-head-left\">Score</th><th>Parent Path</th><th class=\"rounded-head-right\">Filename</th></tr></thead><tbody>")
            i = 0
            lr = len(results)
            for r in results:
                if i == lr - 1:
                    f.write("</tbody><tfoot>")
                    f.write("<tr><td class=\"rounded-foot-left-light\">{}</td><td>{}</td><td class=\"rounded-foot-right-light\">{}</td></tr></tfoot>".format(r[0], r[1], r[2]))
                else:
                    f.write("<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(r[0], r[1], r[2]))
                i += 1
            f.write("</table>") 

            f.write("<h3 class=\"redwood-header\">The top 100 anomalous files</h3>")
            f.write("<table border=\"1\" id=\"redwood-table\">")
            f.write("<thead><tr><th class=\"rounded-head-left\">Anomaly Value</th><th>Parent Path</th><th class=\"rounded-head-right\">Filename</th></tr></thead><tbody>")
            i = 0
            lr = len(anomalies)
            for r in anomalies:
                if i == lr - 1:
                    f.write("</tbody><tfoot>")
                    f.write("<tr><td class=\"rounded-foot-left-light\">{}</td><td>{}</td><td class=\"rounded-foot-right-light\">{}</td></tr></tfoot>".format(r[0], r[1], r[2]))
                else:
                    f.write("<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(r[0], r[1], r[2]))
                i += 1
            #for r in anomalies:
            #    f.write("<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(r[0], r[1], r[2]))
            f.write("</table>") 
        return survey_dir
