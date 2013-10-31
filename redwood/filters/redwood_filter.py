import matplotlib.pyplot as plt
import pylab
import numpy as np
import matplotlib
import array

class RedwoodFilter(object):
    def __init__(self):
        self.name = "generic"
        self.cnx = None    
    def run(self):
        print "running default"
    def clean(self):
        print "cleaning default"
    def update(self, source):
        print "update default"
    def display(self):
        print "displaying"
    
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

    def visualize_histogram(self, data, centroids, xlabel, ylabel):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax_cluster = fig.add_subplot(111)

        ax.hist(data, color = 'b')
        ax_cluster.hist(centroids, color = 'g')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        plt.show()



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





