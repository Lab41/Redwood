import matplotlib.pyplot as plt
import pylab
import numpy as np
import matplotlib

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

    def zip_and_sort(self, code, distances,  sql_results):
       
        combined = list()

        #for  c, d, r in zip(code, distances, sql_results):
        #    combined.append((c,d, r))
        combined = zip(code, distances, sql_results)

        return sorted(combined, key=lambda tup: tup[0])


    def visualize_scatter(self, counts, codes, data, xlabel, ylabel, codebook):

        #set up first
        colorObj = matplotlib.colors.ColorConverter()

        c0 = colorObj.to_rgb('red')
        c1 = colorObj.to_rgb('blue')
        c2 = colorObj.to_rgb('green')
        c3 = colorObj.to_rgb('orange')

        set0 = np.zeros((counts[0], 2))
        set1 = np.zeros((counts[1], 2))
        set2 = np.zeros((counts[2], 2))

        set0_idx = 0
        set1_idx = 0
        set2_idx = 0

        for i, j in zip(codes, data):

            if i == 0:
                curr_set = set0[set0_idx]
                set0_idx+=1
            elif i == 1:
                curr_set = set1[set1_idx]
                set1_idx+=1
            else:
                curr_set = set2[set2_idx]
                set2_idx+=1

            curr_set[0] = j[0]
            curr_set[1] = j[1]

        plt.scatter(codebook[:,0], codebook[:,1], color='orange', s=260)
        plt.scatter(set0[:,0], set0[:,1], color='blue')
        plt.scatter(set1[:,0], set1[:,1], color='green')
        plt.scatter(set2[:,0], set2[:,1], c='cyan')
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





