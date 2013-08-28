#!/usr/bin/python

import time
import mysql.connector
from collections import namedtuple
import numpy as np
from hashlib import sha1
from redwood_connector import RedwoodConn
from scipy.cluster.vq import vq, kmeans, whiten
from collections import defaultdict
import operator

def evaluateDir(dir_name, source, num_clusters, cnx):
       

    cursor = cnx.cursor()
    hash_val = sha1(dir_name).hexdigest()

    query = ("select file_name, file_metadata_id, inode from joined_file_metadata where (source_id = '{}' AND path_hash = '{}')".format(source, hash_val))
    cursor.execute(query)

    sql_results = cursor.fetchall()


    if(len(sql_results) == 0):
        return [None, None]

    inode_arr = np.zeros(len(sql_results))

    
    i=0
    for result in sql_results:
       inode_arr[i] = result[2] 
       i=i+1
    
    whitened = whiten(inode_arr)

    codebook,_ = kmeans(whitened, num_clusters)
    code,dist = vq(whitened, codebook)

    d = defaultdict(int)

    for c in code:
        d[c] += 1
    
    #sorted ascending 
    sorted_codes = sorted(d.iteritems(), key=operator.itemgetter(1)) 
    sorted_results = sortAsClusters(code, sql_results, num_clusters)

    return (whitened, codebook, sorted_codes, sorted_results)

def sortAsClusters(code, sql_results, num_clusters):
   
    combined = list()
    for c, r in zip(sql_results, code):
        combined.append((c, r))
  
    return sorted(combined, key=lambda tup: tup[0])



def findSmallestCluster(code, num_clusters):
    
    l = list()
    
    i=0
    while num_clusters > i:
        l.append([i, 0])
        i=i+1

    for v in code:
        l[v][1] = l[v][1] + 1

    sorted(l, key=lambda x:x[1])
   
    return l;


def evaluateSource(cnx, source_id, num_clusters, threshold):

    cursor = cnx.cursor()

    query = ("select file_metadata_id, full_path, inode from joined_file_metadata where source_id = 1 order by path_hash")
    cursor.execute(query)
    
    curr_path = ""
    count = 0

    files = list()

    for (file_metadata_id, full_path, inode) in cursor:
        if(full_path != curr_path):
            num_obs = len(files)
            if(num_obs > threshold):
                arr = np.zeros(num_obs) 
                i = 0
                for f in files:
                    arr[i] = f
                    i=i+1
                
                files = list()
                whitened = whiten(arr)
                #print whitened
                codebook,_ = kmeans(whitened, num_clusters)
                code,dist = vq(whitened, codebook)
                
                

                #ordered_clusters = findSmallestCluster(code, num_clusters)

                   

                print "Current Directory: {}".format(curr_path)
               
                #print "Codebook: {}".format(codebook)
                #plt.hist(whitened), plt.hist(codebook), plt.show()
            curr_path = full_path
       
        files.append(inode) 
       # print "{} {} {}".format(file_metadata_id, full_path, inode)

