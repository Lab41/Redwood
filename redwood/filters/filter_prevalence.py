from redwood.filters.redwood_filter import RedwoodFilter
import numpy as np

class FilterPrevalence(RedwoodFilter):

    

    def __init__(self):
       # self.supported_viz = [Redwood.histogram]
        self.name = "Prevalence"
        
    def calculate_score(self):
        print "score has been updated!"

    def run(self, cnx):
        print "running prevalence anaylsis..."
        cursor = cnx.cursor()
        query = ("SELECT prevalence_count FROM unique_file")
        cursor.execute(query)
        li = [x[0] for x in cursor.fetchall()]
        cursor.close()

        self.results, self.bin_edges = np.histogram(li, bins=[0, 1, 2, 3, 4, 5])
        print "Results: {}".format(self.results)
        print "Bin Edges: {}".format(self.bin_edges)


