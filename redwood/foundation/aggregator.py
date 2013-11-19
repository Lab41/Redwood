import re

from redwood.filters import plugins

class Aggregator():
    
    def __init_(self, cnx):
        self.cnx = cnx


    #should come in as a:x, b:y, c:z, etc, where x+y+z = 100, and a-c are filter ids
    #standard aggregate is equally weighted
    def aggregate(self, dist_str=None):
        
        weights = list()

        if dist_str is None:
            p = re.compile('\d+:\d+')
            found = p.findall(dist_str)
            for f in found:
                weights.append(f.split(':'))
        else:
            i = 0
            even_split = 100 / len(plugins)
            for p in plugins:
                weights.append((i, even_split))

      
        

        query = """
        UPDATE unique_file  
        LEFT JOIN fp_scores ON fp_scores.id = unique_file.id
        LEFT JOIN lu_scores ON lu_scores.id = unique_file.id
        SET unique_file.reputation = (.5 * fp_scores.score + .5 * lu_scores.score)
        """

        for w in weights:
            fltr = plugins.get(w[0])


