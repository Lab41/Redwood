import matplotlib.pyplot as plt
import pylab
import numpy as np
import matplotlib
import array
from collections import namedtuple

SourceInfo = namedtuple('SourceInfo', 'source_id source_name os_id os_name')

class RedwoodFilter(object):
    def __init__(self):
        self.name = "generic"
        self.cnx = None    
        self.score_table = None
    def run(self):
        print "running default"
    def clean(self):
        print "cleaning default"
    def update(self, source):
        print "update default"
    def display(self):
        print "displaying"
    def rebuild(self):
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
        dir_val = ("desc" if direction is "top" else  "asc")
        
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
        f = self.__getattribute__(func_name)
        f(*args)

    def sortAsClusters(self, code, sql_results):
       
        combined = list()

        for r, c in zip(sql_results, code):
            combined.append((c, r))
      
        return sorted(combined, key=lambda tup: tup[0])

    def visualize_scatter(self, counts, codes, data, codebook, num_clusters, xlabel="", ylabel=""):
        """
        Generates a 2-d scatter plot visualization of two feature data for 

        :param counts: dictionary of counts for the number of observations pairs for 
                        each cluster
        :param codes:  list of codes for each observation row in the order returned by the original query
        :param data: list of observations returned from query in their original order
        :param codebook: the coordinates of the centroids
        :param num_clusters: number of specified clusters up to 8
        :param xlabel: a label for the x axis (Default: None)
        :param ylabel: a label for the y axis (Default: None)
        """
        if num_clusters > 8:
            print "Visualize scatter only supports up to 8 clusters"
            return

        num_features = 2
        list_arrays = list()
        list_arr_idx = array.array("I", [0, 0, 0])

        for idx in range(num_clusters):
            list_arrays.append(np.zeros((counts[idx], num_features)))


        for i, j in zip(codes, data):

            list_arrays[i][list_arr_idx[i]][0] = j[0]
            list_arrays[i][list_arr_idx[i]][1] = j[1]
            list_arr_idx[i] += 1

        #plot the clusters first as relatively larger circles
        plt.scatter(codebook[:,0], codebook[:,1], color='orange', s=260)
       
        colors = ['red', 'blue', 'green', 'purple', 'cyan', 'black', 'brown', 'grey']

        for idx in range(num_clusters):
            plt.scatter(list_arrays[idx][:,0], list_arrays[idx][:,1], c=colors[idx]) 
        
        plt.ylabel(ylabel)
        plt.xlabel(xlabel)
        plt.show()

    def visualize_histogram(self, data, centroids=None, xlabel=None, ylabel=None):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.hist(data, color = 'b')
        
        if centroids is not None:
            ax_cluster = fig.add_subplot(111)
            ax_cluster.hist(centroids, color = 'g')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        plt.show()


    def get_source_info(self, source_name):
        
        cursor = self.cnx.cursor()
        
        query = """
            SELECT media_source.id as source_id, media_source.name as source_name, os.id as os_id, os.name as os_name
            FROM redwood.media_source LEFT JOIN os ON media_source.os_id = os.id where media_source.name = "{}";
        """.format(source_name)
        
        cursor.execute(query)
        r =  cursor.fetchone()
        
        if r is None:
            return r

        return SourceInfo(r[0], r[1], r[2],r[3])

    def get_num_systems(self, os_name_or_id):
        """

        """

        if os_name_or_id.isdigit() is False:
           os_id = "(SELECT DISTINCT os.id from os where os.name = \"{}\")".format(os_name_or_id)

        cursor = self.cnx.cursor()
        
        query = """
            SELECT COUNT(media_source.id) FROM os 
            LEFT JOIN media_source ON os.id = media_source.os_id
            WHERE os.id = {}
            GROUP BY os.id
        """.format(os_id)
        
        cursor.execute(query)
        r = cursor.fetchone()
        return r[0]

    def get_source_id(self, source_name):
        
        cursor = self.cnx.cursor()
        
        cursor.execute(("""select id from media_source where name = '{}';""").format(source_name))
        
        r = cursor.fetchone()
        
        if r is None:
            print "Error: Source with name \"{}\" does not exist".format(source_name)
            return -1

        return r[0]
        


    def findSmallestCluster(self, code, num_clusters):
        
        l = list()
        
        i=0
        while num_clusters > i:
            l.append([i, 0])
            i=i+1

        for v in code:
            l[v][1] = l[v][1] + 1

        sorted(l, key=lambda x:x[1])
       
        return l;


    def table_exists(self, name):
        cursor = self.cnx.cursor()
        result = None
        try:
            cursor.execute("select COUNT(id) from {}".format(name))
            result = cursor.fetchone()
            cursor.close()
        except Exception as err:
            pass

       
        if(result == None or result[0] == 0):
            return False
        else: 
            return True




