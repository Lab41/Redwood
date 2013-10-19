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

warnings.filterwarnings('ignore')

def find_anomalies(rows, sorted_results, sorted_code_counts):
    distance_threshold0 = 1.0
    distance_threshold1 = 1.25
    distance_threshold2 = 1.5

   #print "Code counts: {} smallest: {} ".format(sorted_code_counts, sorted_code_counts[0][0])
    smallest_count = sorted_code_counts[0][1]
    if smallest_count < 2:
        target = sorted_code_counts[0][0]  #smallest cluster
    else:
        target = -1


    for c, d, r in sorted_results:

        #a lone cluster with just 1 element
        if c == target:
            score = .3
        if d > distance_threshold2:
            score = .1
        elif d> distance_threshold1:
            score = .3
        elif d> distance_threshold0:
            score = .4
        else:
            score = 1
        file_metadata_id = r[0]
        rows.put((file_metadata_id, score))


def do_eval(rows, full_path, files, num_clusters, num_features):
    
    srt = time.time()
    num_obs = len(files)
   
    if(num_obs < num_clusters):
        return
 
    #zero out the array that will contain the inodes
    observations = np.zeros((num_obs, num_features))

    i = 0
    
    for inode,mod_date,_,_,_,_, in files:
        seconds = calendar.timegm(mod_date.utctimetuple())
        observations[i][0] = inode
        observations[i][1] = seconds
        i += 1
    
    whitened = whiten(observations) 

    #get the centroids
    codebook,_ = kmeans(whitened, num_clusters)
   
    #sometimes if all observations for a given feature are the same
    #the centroids will not be found.  For now, just throw out that data, but
    #figure out a solution later
    if len(codebook) != num_clusters:
        files = list()
        return

    code, dist = vq(whitened, codebook)
    d = defaultdict(int)

    #quick way to get count of cluster sizes        
    for c in code:
        d[c] += 1


    #sorted the map codes to get the smallest to largest cluster
    sorted_codes = sorted(d.iteritems(), key = operator.itemgetter(1))
    
    combined = zip(code, dist, files)
    sorted_results =  sorted(combined, key=lambda tup: tup[0])
    
    find_anomalies(rows, sorted_results, sorted_codes) 
     
    elp = time.time() - srt
    #print "completed pid {} time: {}".format(os.getpid(), elp)
             



class LocalityUniqueness(RedwoodFilter):

    def __init__(self):
        self.cnx = None
        self.name = "Locality Uniqueness"

    def usage(self):
        print "Evaluate Directory requires "

    def update(self, source):
        self.build()
        self.evaluateSource(source, 3)
       

    def discover_show_top(self, n):
        cursor = self.cnx.cursor()
        query = ("""SELECT lu_scores.id, score,  unique_path.full_path, file_metadata.file_name, media_source.name from lu_scores 
                    LEFT JOIN file_metadata ON (lu_scores.id = file_metadata.unique_file_id) LEFT JOIN unique_path on 
                    (file_metadata.unique_path_id = unique_path.id) 
                    LEFT JOIN media_source on (file_metadata.source_id = media_source.id) order by score desc limit 0, {}""").format(n)
        cursor.execute(query)
        for (index, score, path, filename, source) in cursor:
            print "{} {} {} {} {}".format(index, score, path, filename, source)



    def discover_show_bottom(self, n):
        
        cursor = self.cnx.cursor()
        query = ("""SELECT lu_scores.id, score,  unique_path.full_path, file_metadata.file_name, media_source.name from lu_scores 
                    LEFT JOIN file_metadata ON (lu_scores.id = file_metadata.unique_file_id) LEFT JOIN unique_path on 
                    (file_metadata.unique_path_id = unique_path.id) 
                    LEFT JOIN media_source on (file_metadata.source_id = media_source.id) order by score asc limit 0, {}""").format(n)
        cursor.execute(query)
        for(index, score, path, filename, source) in cursor:
            print "{} {} {} {} {}".format(index, score, path, filename, source)


    def discover_evaluate_dir(self, dir_name, source, num_clusters):
        
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

    def discover_evaluateSource(self, source_id, num_clusters):
        self.evaluateSource(self.cnx, source_id, num_clusters)
    
    def evaluateSource(self, source_name, num_clusters):

        cursor = self.cnx.cursor()
        
        cursor.execute(("""select id from media_source where name = '{}';""").format(source_name))
        
        r = cursor.fetchone()
        
        if r is None:
            print "Error: Source with name \"{}\" does not exist".format(source_name)
            return

        source_id = r[0]
        query = ("""SELECT count(*) from lu_analyzed_sources
                where id = '{}';""").format(source_id)

        cursor.execute(query)

        found = cursor.fetchone() 
        if found[0] > 0:
            print "already analyzed source {}".format(source_name)
            return
        else:
            query = "insert into lu_analyzed_sources (id, name) VALUES('{}','{}')".format(source_id, source_name)
            cursor.execute(query)
            self.cnx.commit()
            print "Evaluating {} ".format(source_name)
        #returns all files sorted by directory for the given source
        query = ("""SELECT file_metadata_id, last_modified, full_path, file_name, filesystem_id, parent_id 
                FROM joined_file_metadata 
                where source_id = {} order by parent_id asc""").format(source_id)
        
        cursor.execute(query)

        files = list()
        first = cursor.fetchone()
        if first is None:
            return

        files.append(first)
        parent_id_prev = first[5]

        print "...Beginning clustering analysis"

        pool = Pool(processes=4)              # start 4 worker processes

        manager = Manager()
        
        rows = manager.Queue()


        
        #should iterate by dir of a given source at this point
        for(file_metadata_id, last_modified, full_path, file_name, filesystem_id, parent_id) in cursor:
           
            if parent_id_prev != parent_id:
                parent_id_prev = parent_id
                
                pool.apply_async(do_eval, [rows, full_path, files, num_clusters, 2])
                files = list()
            else:
                #make sure to omit directories
                if file_name != '/':
                    files.append((file_metadata_id, last_modified, full_path,file_name, filesystem_id, parent_id))
      
        pool.close()
        pool.join() 
           
        input_rows = []
        while rows.empty() is False:
            curr = rows.get()
            input_rows.append(curr)
        print "...sending results to server"
        
        cursor.executemany("""INSERT INTO locality_uniqueness(file_metadata_id, score) VALUES(%s, %s)""", input_rows)
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
        query = ("""INSERT INTO lu_scores
                    (SELECT file_metadata.unique_file_id, avg(locality_uniqueness.score) FROM 
                    locality_uniqueness LEFT JOIN file_metadata on (locality_uniqueness.file_metadata_id = file_metadata.id) 
                    GROUP BY file_metadata.unique_file_id)""")            
       
        cursor.execute(query)
        self.cnx.commit()
        

         

    def clean(self):
        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS lu_scores")
        cursor.execute("DROP TABLE IF EXISTS locality_uniqueness")
        cursor.execute("DROP TABLE IF EXISTS lu_analyzed_sources")
        self.cnx.commit()


    def build(self):

        cursor = self.cnx.cursor()

        query = ("CREATE TABLE IF NOT EXISTS lu_analyzed_sources ( "
                "id INT UNSIGNED NOT NULL,"
                "name VARCHAR(45) NOT NULL,"
                "PRIMARY KEY(id),"
                "CONSTRAINT fk_media_source_id FOREIGN KEY (id)"
                "REFERENCES media_source(id)"
                "ON DELETE NO ACTION ON UPDATE NO ACTION"
                ") ENGINE=InnoDB;")

        cursor.execute(query)
       
        self.cnx.commit()

        query = ("CREATE table IF NOT EXISTS locality_uniqueness ("
                "file_metadata_id BIGINT unsigned unique,"
                "score DOUBLE NOT NULL,"
                "PRIMARY KEY(file_metadata_id),"
                "INDEX lu_score (score ASC),"
                "CONSTRAINT fk_file_metadata FOREIGN KEY (file_metadata_id)"
                "REFERENCES file_metadata (id)"
                "ON DELETE NO ACTION ON UPDATE NO ACTION"
                 ") ENGINE = InnoDB;")
        
        cursor.execute(query)
        
        self.cnx.commit()
    

