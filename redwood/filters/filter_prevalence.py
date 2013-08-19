from redwood.filters.redwood_filter import RedwoodFilter
import numpy as np

class FilterPrevalence(RedwoodFilter):

    

    def __init__(self):
        self.name = "Prevalence"
        

    def calculate_score(self):
        print "score has been updated!"

    def run(self):
        print "running prevalence anaylsis..."
        cursor = cnx.cursor()
        query = ("SELECT prevalence_count FROM unique_file")
        cursor.execute(query)
        li = [x[0] for x in cursor.fetchall()]
        cursor.close()

        self.results, self.bin_edges = np.histogram(li, bins=[0, 1, 2, 3, 4, 5])
        print "Results: {}".format(self.results)
        print "Bin Edges: {}".format(self.bin_edges)

    def run_query(self):
        cursor = self.cnx.cursor()

        
        #we can probably get the count of the os too my implementing the os table as seen in the current sqsl script
        query = ("INSERT INTO filter_prevalence(unique_file_id, count, num_systems, os_id) "
                    "SELECT  unique_file_id, COUNT(DISTINCT unique_file_id, media_source.id) as count, num_systems, s.idx "
                    "from file_metadata LEFT JOIN media_source ON (file_metadata.source_id = media_source.id) "
                        "LEFT JOIN( select os.id as idx, os.name as os, COUNT(os.name) as num_systems "
                        "from os LEFT JOIN media_source ON(os.id = media_source.os_id) GROUP BY os.name ) s "
                        "ON (s.idx = file_metadata.os_id) "
                        "GROUP BY unique_file_id;")


        cursor.execute(query)
        self.cnx.commit()
        cursor.close()

    def build_table(self):
        cursor = self.cnx.cursor()

        query = ("CREATE TABLE IF NOT EXISTS filter_prevalence ( "
                "id INT NOT NULL AUTO_INCREMENT, "
                "score DOUBLE NOT NULL DEFAULT .5, "
                "unique_file_id INT NOT NULL, "
                "count INT NOT NULL DEFAULT 0,"
                "num_systems INT NOT NULL DEFAULT 0,"
                "os_id INT NOT NULL, " 
                "PRIMARY KEY(id), "
                "INDEX fk_unique_file_idx (unique_file_id), "
                "INDEX fk_os_id_idx (os_id),"
                "CONSTRAINT fk_unique_file_idx FOREIGN KEY(unique_file_id) "
                "REFERENCES unique_file (id) "
                "ON DELETE NO ACTION ON UPDATE NO ACTION,"
                "CONSTRAINT fk_os_id_idx FOREIGN KEY(os_id) "
                "REFERENCES os (id) "
                "ON DELETE NO ACTION ON UPDATE NO ACTION "
                ") ENGINE = InnoDB;")
        
        cursor.execute(query)
        self.cnx.commit()
        cursor.close()
