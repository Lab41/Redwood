import matplotlib.pyplot as plt

class RedwoodFilter(object):
    def __init__(self):
        self.name = "generic"
        self.cnx = None    
    def run(self, cnx):
        print "running default"
    def display(self):
        print "displaying"

    def sortAsClusters(self, code, sql_results, num_clusters):
       
        combined = list()

        for r, c in zip(sql_results, code):
            combined.append((c, r))
      
        return sorted(combined, key=lambda tup: tup[0])


    def visualize_histogram(self, data, centroids, xlabel, ylabel):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax_cluster = fig.add_subplot(111)

        ax.hist(data, color = 'b')
        ax_cluster.hist(centroids, color = 'g')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        plt.show()



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





