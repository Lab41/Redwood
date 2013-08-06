from redwood.filters import RedwoodFilter

class FilterPrevalence(RedwoodFilter):

    def __init__(self, cnx):
        self.cnx = cnx
        self.supported_viz = [Redwood.histogram]
        self.name = "Prevalence"

    def calculate_score(self):
        print "score has been updated!"

    def as_histogram(self):

        cursor = self.cnx.cursor()
        query = ("SELECT prevalence_count FROM unique_file")
        cursor.execute(query)
        li = [x[0] for x in cursor.fetchall()]
        cursor.close()

        results, bin_edges = np.histogram(li, bins=[0, 1, 2, 3, 4, 5])

        return results, bin_edges

    

