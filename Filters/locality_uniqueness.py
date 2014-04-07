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


import time
import operator
import os
import numpy as np
import matplotlib.pyplot as plt
from collections import namedtuple, defaultdict
from hashlib import sha1
from scipy.cluster.vq import vq, kmeans, whiten
from redwood.filters.redwood_filter import RedwoodFilter
import calendar
import random
import warnings
from multiprocessing import Pool, Queue, Manager
import Queue
import redwood.helpers.core as core
import redwood.helpers.visual as visual
import shutil

warnings.filterwarnings('ignore')

#NOTE: the find_anomalies and do_eval functions are outside the class so that we
#can run them in parallel using the apply_async function for thread pools

SMALL_CLUSTERS_SCORE = .3
DEFAULT_NUM_CLUSTERS = 3

def find_anomalies(rows, sorted_results, code_count_dict):
    """
    Helper function that given a list of results from kmeans will assign
    scores to each file given their distance form their centroid

    :param rows: output rows to append to
    :param sorted_rows: results from kmeans sorted by first column of their code id
    :sorted_code_counts: centroids sorted by number of observations
    """
    #definitely want to adjust these distance thresholds
    distance_threshold0 = 1.0
    distance_threshold1 = 2.0
    distance_threshold2 = 5.0
    distance_threshold3 = 10.0

    #assign scores based on distance 
    for c, d, r in sorted_results:

        #get code count
        code_count = code_count_dict[c]

        #if a file belongs to a cluster with fewer than three elements, we automatically assign in lower score
        if code_count < 3:
            score = SMALL_CLUSTERS_SCORE
        elif d > distance_threshold3:
            score = .1
        elif d > distance_threshold2:
            score = .2
        elif d> distance_threshold1:
            score = .3
        elif d> distance_threshold0:
            score = .4
        else:
            score = .8
        file_metadata_id = r[0]
        rows.put((file_metadata_id, score))


def do_eval(rows, full_path, files, num_clusters, num_features):
    """
    Helper function that analyzes a directory, looking for outliers in clusters based on the input features.
    Currently, only two static features are analyzed, however future versions could allow
    for selectable set of features

    :param rows: output variable to append results to
    :param full_path: the path that is being analyzed
    :param files: meta data for files in the directory
    :param num_clusters: number of clusters to specify for kmeans
    :param num_features: number of features included

    :return: nothing... use the rows input as an output to append to
    """

    num_obs = len(files)

    #if the number of observations is less than the num_clusters we do not cluster
    #but rather give each file SMALL CULSTER SCORE
    if(num_obs < num_clusters):
        for f in files:
            rows.put((f[0], SMALL_CLUSTERS_SCORE))
        return

    #zero out the two dimensional array
    observations = np.zeros((num_obs, num_features))

    i = 0

    #transfer the observations to the numpy array
    for file_metadata_id,mod_date,full_path,file_name,inode,parent_id, in files:
        seconds = calendar.timegm(mod_date.utctimetuple())
        observations[i] = (inode, seconds)
        i += 1

    #normalize the observations
    whitened = whiten(observations)

    #get the centroids (aka codebook)
    codebook,_ = kmeans(whitened, num_clusters)

    #sometimes if all observations for a given feature are the same
    #the centroids will not be found. In that case we give a neutral score
    if len(codebook) != num_clusters:
        for f in files:
            rows.put((f[0], .5))
        return

    #calulate the distances
    code, dist = vq(whitened, codebook)


    d = defaultdict(int)

    #quick way to get count of cluster sizes
    for c in code:
        d[c] += 1

    #combine the results with the original data, then sort by the code
    combined = zip(code, dist, files)
    sorted_results =  sorted(combined, key=lambda tup: tup[0])

    find_anomalies(rows, sorted_results, d)




class LocalityUniqueness(RedwoodFilter):
    """
    LocalityUniqueness seeks to identify anomalies through clustering of file features in a given directory. The
    assumption is that files of interest are those that are different than most of their neighbors in a given
    domain -- this case being the directory.  As a result, this filter is responsible for giving outliers of clusters
    lower reputation scores than those files closer to the centroid
    """

    def __init__(self, cnx=None):
        self.score_table = "lu_scores"
        self.name = "Locality_Uniqueness"
        self.cnx = cnx

    def usage(self):
        """
        Prints the usage statements for the discovery functions
        """
        print "[*] evaluate_dir [full_path] [source] [clusters]"
        print "\t|- runs kmeans and shows scatter plot"
        print "\t| [full_path]  - path to analyze"
        print "\t| [source]     - source where the path exists"
        print "\t| [clusters]   - number of clusters to use"

    def update(self, source):
        """
        Applies the Locality Uniqueness filter to the given source, updating existing data
        analyzed from previous sources. Currently the update function uses 3 clusters for clustering
        analysis.  This will be dynamic in future versions.

        :param source: media source name
        """
        print "[+] Locality Uniqueness Filter running on {}".format(source)
        self.build()
        self.evaluate_source(source)

    def evaluate_source(self, source_name, num_clusters=DEFAULT_NUM_CLUSTERS):
        """
        Evaluates and scores a given source with a specified number of clusters for kmeans. Currently
        this function uses two set features as inputs (modification time and inode number), however
        futures versions will allow for dynamic feature inputs

        :param source_name: media source name
        :param num_clusters: number of clusters to input into kmeans (Default: 3)
        """

        cursor = self.cnx.cursor()
        src_info = core.get_source_info(self.cnx, source_name)

        #returns all files sorted by directory for the given source
        #query = """
        #    SELECT file_metadata_id, last_modified, full_path, file_name, filesystem_id, parent_id, hash
        #    FROM joined_file_metadata
        #    where source_id = {} order by parent_id asc
        #    """.format(src_info.source_id)

        cursor.execute("""
                       SELECT file_metadata_id, last_modified, full_path, file_name, filesystem_id, parent_id, hash
                       FROM joined_file_metadata
                       where source_id = %s order by parent_id asc
                       """, (src_info.source_id,))

        files = list()

        print "...Beginning clustering analysis"
        pool = Pool(processes=4)              # start 4 worker processes
        manager = Manager()
        rows = manager.Queue()
        is_first = True

        parent_id_prev = None
        #should iterate by dir of a given source at this point
        for(file_metadata_id, last_modified, full_path, file_name, filesystem_id, parent_id, hash_val) in cursor:

            if is_first is True:
                is_first = False
                parent_id_prev = parent_id

            #if parent_id is diff than previous, we are in new directory, so pack it up for analysis
            if parent_id_prev != parent_id:

                parent_id_prev = parent_id

                if len(files) > 0:
                    pool.apply_async(do_eval, [rows, full_path, files, num_clusters, 2])
                    files = list()

            #make sure to omit directories from the clustering analy
            if file_name != '/' and hash_val != "":
                files.append((file_metadata_id, last_modified, full_path,file_name, filesystem_id, parent_id))

        if len(files) > 0:
            pool.apply_async(do_eval, [rows, full_path, files, num_clusters, 2])

        pool.close()
        pool.join() 

        input_rows = []
        count = 0
        while rows.empty() is False:
            curr = rows.get()
            input_rows.append(curr)
            count +=1
            if count % 50000 is 0:
                print "...sending {} results to server".format(len(input_rows))
                cursor.executemany("""REPLACE INTO locality_uniqueness(file_metadata_id, score) values(%s, %s)""", input_rows)
                input_rows = []
                count=0
        print "...sending {} results to server".format(len(input_rows))

        cursor.executemany("""REPLACE INTO locality_uniqueness(file_metadata_id, score) values(%s, %s)""", input_rows)
        self.cnx.commit()
        #need to drop the lu_scores and recalculate
        cursor.execute("drop table if exists lu_scores")

        query = ("""CREATE TABLE IF NOT EXISTS `lu_scores` (
                `id` bigint(20) unsigned NOT NULL,
                `score` double DEFAULT NULL,
                KEY `fk_unique_file0_id` (`id`),
                CONSTRAINT `fk_unique_file0_id` FOREIGN KEY (`id`)
                REFERENCES `unique_file` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
                        ) ENGINE=InnoDB""")

        cursor.execute(query)

        print "...updating scores on the server"
        query = """
            INSERT INTO lu_scores
            (SELECT file_metadata.unique_file_id, avg(locality_uniqueness.score) FROM
            locality_uniqueness LEFT JOIN file_metadata on (locality_uniqueness.file_metadata_id = file_metadata.id)
            WHERE file_metadata.unique_file_id is not null
            GROUP BY file_metadata.unique_file_id)
            """

        cursor.execute(query)
        self.cnx.commit()


    def clean(self):
        """
        Removes all tables associated with this filter
        """

        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS lu_scores")
        cursor.execute("DROP TABLE IF EXISTS locality_uniqueness")
        self.cnx.commit()


    def build(self):
        """
        Build all persistent tables associated with this filter
        """
        cursor = self.cnx.cursor()

        query = """
            CREATE table IF NOT EXISTS locality_uniqueness (
            file_metadata_id BIGINT unsigned unique,
            score DOUBLE NOT NULL,
            PRIMARY KEY(file_metadata_id),
            INDEX lu_score (score ASC),
            CONSTRAINT fk_file_metadata11 FOREIGN KEY (file_metadata_id)
            REFERENCES file_metadata (id)
            ON DELETE NO ACTION ON UPDATE NO ACTION
            ) ENGINE = InnoDB;
        """

        cursor.execute(query)

        self.cnx.commit()

    ##################################################
    #
    #       DISCOVERY FUNCTIONS
    #
    ##################################################

    def discover_evaluate_dir(self, dir_name, source, num_clusters=DEFAULT_NUM_CLUSTERS):
        """
        Discovery function that applies kmeans clustering to a specified directory, displays
        the resulting scatter plot with the clusters, and then prints out an ordered list of
        the file by the distance from their respective centroid. Currently,
        this function uses two static features of "modification date" and "inode number" but
        future versions will allow for dynamic features inputs.

        :param dir_name: directory name to be analyzed (Required)
        :source: source name to be analzyed (Required)
        :num_clusters: specified number of clusters to use for kmeans (Default: 3)
        """

        num_features = 2
        num_clusters = int(num_clusters)
        cursor = self.cnx.cursor()

        if(dir_name.endswith('/')):
            dir_name = dir_name[:-1]

        print "...Running discovery function on source {} at directory {}".format(source, dir_name)

        src_info = core.get_source_info(self.cnx, source)
        if src_info is None:
            print "Error: Source {} does not exist".format(source)
            return

        #grab all files for a particular directory from a specific source
        hash_val = sha1(dir_name).hexdigest()

        #query = """
        #    SELECT file_name, file_metadata_id, filesystem_id, last_modified
        #    FROM joined_file_metadata
        #    WHERE source_id ='{}' AND path_hash = '{}' AND file_name !='/'
        #    """.format(src_info.source_id, hash_val)

        cursor.execute("""
                       SELECT file_name, file_metadata_id, filesystem_id, last_modified
                       FROM joined_file_metadata
                       WHERE source_id = %s AND path_hash = %s AND file_name !='/'
                       """, (src_info.source_id, hash_val,))

        #bring all results into memory
        sql_results = cursor.fetchall()

        if(len(sql_results) == 0):
            return

        print "...Found {} files in specified directory".format(len(sql_results))
        print "...Will form into {} clusters".format(num_clusters)
        if num_clusters > len(sql_results):
            print "Number of clusters ({}) exceeds number of files ({})".format(num_clusters, len(sql_results))
            num_clusters = len(sql_results)
            print "Number of clusters is now: {}".format(num_clusters)


        #zero out the array that will contain the inodes
        filesystem_id_arr = np.zeros((len(sql_results), num_features))

        i = 0
        for _, _,inode, mod_date in sql_results:
            seconds = calendar.timegm(mod_date.utctimetuple())
            filesystem_id_arr[i] = (inode, seconds)
            i += 1
        whitened = whiten(filesystem_id_arr)
        #get the centroids
        codebook,_ = kmeans(whitened, num_clusters)
        code, dist = vq(whitened, codebook)
        d = defaultdict(int)

        #quick way to get count of cluster sizes
        for c in code:
            d[c] += 1

        #sorts the codes and sql_results together as pairs
        combined = zip(dist, code, sql_results)

        #sort results by distances from centroid
        sorted_results =  sorted(combined, key=lambda tup: tup[0])

        for dist_val, c, r in sorted_results:
            print "Dist: {} Cluster: {}  Data: {}".format(dist_val,c,r)


        if codebook is None or len(codebook) == 0:
            print "Data is not suitable for visualization"
            return

        visual.visualize_scatter(d, code, whitened, codebook, num_clusters, "inode number", "modification datetime", dir_name)


    ##################################################
    #
    #       SURVEY
    #
    ##################################################

    def run_survey(self, source_name):
        """
        Runs survey for this filter capturing discovery functions and reputation score results

        :param source_name: name of the source to survey
        :return survey_dir: location where survey results were saved
        """

        print "...running survey for {}".format(self.name)

        resources = "resources"
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

        results = self.show_results("bottom", 100, source_name, None)

        with open(html_file, 'w') as f:

            f.write("""
            <html>
            <head>
            <link href="../../../resources/css/style.css" rel="stylesheet" type="text/css">
            </head>
            <body>
            <h2 class="redwood-title">Locality Uniqueness Snapshot</h2>
            """)
            f.write("<h3 class=\"redwood-header\">The lowest 100 reputations for this filter</h3>")
            f.write("<table border=\"1\" id=\"redwood-table\">")
            f.write("<thead>")
            f.write("<tr><th class=\"rounded-head-left\">Score</th><th>Parent Path</th><th class=\"rounded-head-right\">Filename</th></tr>")
            f.write("</thead><tbody>")
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

            f.write("</body></html>")
            return survey_dir
