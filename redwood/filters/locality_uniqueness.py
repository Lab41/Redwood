import time
import operator
import numpy as np
import matplotlib.pyplot as plt
from collections import namedtuple, defaultdict
from hashlib import sha1
from scipy.cluster.vq import vq, kmeans, whiten
from redwood.filters.redwood_filter import RedwoodFilter

import random

class LocalityUniqueness(RedwoodFilter):

    def __init__(self):
        self.cnx = None
        self.name = "Locality Uniqueness"

    def usage(self):
        print "Evaluate Directory requires "

    #def run(self):
    
    def discover_evaluateDir(self, dir, source, num_clusters):
        whitened, codebook, sorted_codes, sorted_results = self.evaluateDir(dir, source, num_clusters, self.cnx)

    def evaluate_dir(self, dir_name, source, num_clusters):
        
        cursor = self.cnx.cursor()
        
        #grab all files for a particular directory from a specific source
        hash_val = sha1(dir_name).hexdigest()
        
        query = ("""SELECT file_name, file_metadata_id, filesystem_id
        FROM joined_file_metadata
        WHERE source_id = (select id from media_source where name = '{}') AND path_hash = '{}'""").format(source, hash_val)

        cursor.execute(query)

        #bring all results into memory
        sql_results = cursor.fetchall()

        if(len(sql_results) == 0):
            return [None, None]
       
        #zero out the array that will contain the inodes
        filesystem_id_arr = np.zeros(len(sql_results))

        i = 0
        for _, _, inode in sql_results:
            filesystem_id_arr[i] = inode
            i += 1

        whitened = whiten(filesystem_id_arr) 

        #get the centroids
        codebook,_ = kmeans(whitened, num_clusters)
        code, dist = vq(whitened, codebook)
        print code
        d = defaultdict(int)

        #quick way to get count of cluster sizes        
        for c in code:
            d[c] += 1
      
        #sorted the map codes to get the smallest to largest cluster
        sorted_codes = sorted(d.iteritems(), key = operator.itemgetter(1))
        #sorts the codes and sql_results together as pairs
        sorted_results = self.sortAsClusters(code, sql_results, num_clusters)
         
        print sorted_results
        self.visualize_histogram(whitened, codebook, "normalized inodes", "file occurrences")

        return (sorted_codes, sorted_results)

    def discover_evaluateSource(self, source_id, num_clusters, threshold):
        self.evaluateSource(self.cnx, source_id, num_clusters, threshold)

    def evaluateSource(self, cnx, source_id, num_clusters, threshold):
        cursor = cnx.cursor()
        query = ("""SELECT file_metadata_id, full_path, filesystem_id
        FROM joined_file_metadata
        WHERE source_id = 1 ORDER BY path_hash""")
        cursor.execute(query)

        curr_path = ""
        count = 0

        files = list()

        for(file_metadata_id, full_path, filesystem_id) in cursor:
            if(full_path != curr_path):
                num_obs = len(files)
                if(num_obs > threshold):
                    arr = np.zeros(num_obs)
                    i = 0
                    for f in files:
                        arr[i] = f
                        i += 1

                    files = list()
                    whitened = whiten(arr)
                    #print whitened
                    codebook, _ = kmeans(whitened, num_clusters)
                    code, dist = vq(whitened, codebook)

                    #ordered_clusters = findSmallestCluster(code, num_clusters)

                    print "Current Directory: {}".format(curr_path)

                    #print "Codebook: {}".format(codebook)
                    #plt.hist(whitened), plt.hist(codebook), plt.show()

                curr_path = full_path

            files.append(filesystem_id)
<<<<<<< HEAD



    def build(self):

        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS filter_loc_uniqueness")
        self.cnx.commit()
        query = ("CREATE TABLE IF NOT EXISTS locality_uniqueness ( "
                    "global_file_id BIGINT UNSIGNED NOT NULL,"
                    "score DOUBLE NOT NULL DEFAULT .5, "
                    "os_id INT UNSIGNED NOT NULL "
                    ") ENGINE = InnoDB;")

                
        print "Building Locality Uniqueness staging table"
    

=======
>>>>>>> 0976f65fb7bc5e36bd56b0bed4b77995788e7173
