import time
import mysql.connector
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

    def evaluateDir(self, dir_name, source, num_clusters, cnx):
        cursor = cnx.cursor()
        hash_val = sha1(dir_name).hexdigest()
        query = ("""SELECT file_name, file_metadata_id, filesystem_id
        FROM joined_file_metadata
        WHERE source_id = '{}' AND path_hash = '{}'""").format(source, hash_val)

        cursor.execute(query)

        sql_results = cursor.fetchall()

        if(len(sql_results) == 0):
            return [None, None]

        filesystem_id_arr = np.zeros(len(sql_results))

        i=0
        for result in sql_results:
            filesystem_id_arr[i] = result[2]
            i += 1
        whitened = whiten(filesystem_id_arr)
        whitened = np.sort(whitened)
        filesystem_id_arr = np.sort(filesystem_id_arr)

        codebook,_ = kmeans(whitened, num_clusters)
        code, dist = vq(whitened, codebook)
        d = defaultdict(int)

        for c in code:
            d[c] += 1

        #sorted ascending
        sorted_codes = sorted(d.iteritems(), key = operator.itemgetter(1))
        sorted_results = self.sortAsClusters(code, sql_results, num_clusters)

        bins = list()
        i = 1
        while(i <= len(whitened)):
            bins.insert(i, i)
            i+=1

        x = [random.gauss(3, 1) for _ in range(400)]
        y = [random.gauss(4, 2) for _ in range(400)]


        weighted_centroids = list()
        tmp = list()

        for i in range(0, len(codebook)):
            tmp.append(codebook[i])

        for i in range(0, (len(whitened) / 100)):
            weighted_centroids.extend(tmp)

        fig = plt.figure()
        ax = fig.add_subplot(111)
        #plt.hist(whitened, whitened.max() * 2, [0, whitened.max()], color = 'b', alpha = 0.5)
        ax.hist(whitened, bins = 15, color = 'b')
        ax2 = fig.add_subplot(111)
        #plt.hist(test, num_clusters * 4, [0, num_clusters * 2], color='g', alpha = 0.5)
        ax2.hist(weighted_centroids, color = 'g')
        ax.set_xlabel("filesystem ID's")
        ax.set_ylabel("file occurences")
        plt.show()

        #plt.hist(whitened, whitened.max() * 2, [0, whitened.max()], color = 'b'),plt.hist(codebook, 6, [0, whitened.max()], color='g'),plt.show()

        return (whitened, codebook, sorted_codes, sorted_results)

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