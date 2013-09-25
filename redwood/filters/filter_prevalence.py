from redwood.filters.redwood_filter import RedwoodFilter
import numpy as np
import matplotlib.pyplot as plt

class FilterPrevalence(RedwoodFilter):

    

    def __init__(self):
        self.name = "Prevalence"
        self.cnx = None         

    def usage(self):
        print "view_high <count>"
        print "\t-displays top <count> scores for this filter"
        print "view_low <count>"
        print "\t-displays lowest <count> score for this filter"

    def discover_histogram(self, os_type):
        if(self.table_exists() == False):
            self.run()
        
        

        cursor = self.cnx.cursor()
        #first get the count for this os
        query = ("select COUNT(id) from media_source where os_id = "
                "(SELECT id from os where (name = '{}'))").format(os_type)

        cursor.execute(query)
        results = cursor.fetchone()

        num_systems = results[0]
        print num_systems

        print "running prevalence anaylsis..."
        cursor = self.cnx.cursor()
        
        query = ("SELECT count FROM filter_prevalence where os_id = "
          "(SELECT id from os where (name = '{}'))").format(os_type)

        cursor.execute(query)
        
        li = [x[0] for x in cursor.fetchall()]
        cursor.close()

        bins = list()
        i = 1
        while(i <= num_systems):
            bins.insert(i, i)
            i+=1
        fig = plt.figure()
        ax = fig.add_subplot(111)
        pdf, bins, patches = ax.hist(li, bins=bins)
        ax.set_xlabel("systems")
        ax.set_ylabel("file occurences")
        ax.set_xlim(1, num_systems)
        plt.show()


    def discover_view_high(self, count):
        if(self.table_exists() == False):
            self.run()
         
        cursor = self.cnx.cursor()
        
        query = ("select full_path, file_name, score, file_type " 
                "from joined_file_metadata JOIN filter_prevalence "
                "ON (joined_file_metadata.unique_file_id = filter_prevalence.unique_file_id) "
                "where (joined_file_metadata.file_type != '_dir') "
                "ORDER BY score DESC "
                "limit 0 , {}").format(count)

        cursor.execute(query)
        v = 0
        for x in cursor.fetchall():
            print "{}:{} ({}) {}{}".format(v, x[2], x[3], x[0], x[1])
            v += 1 
        
        cursor.close()
        
        

    def discover_view_low(self, count):
        
        if(self.table_exists() == False):
            self.run()
         
        cursor = self.cnx.cursor()
        
        query = ("select full_path, file_name, score, file_type " 
                "from joined_file_metadata JOIN filter_prevalence "
                "ON (joined_file_metadata.unique_file_id = filter_prevalence.unique_file_id) "
                "where (joined_file_metadata.file_type != '_dir') "
                "ORDER BY score ASC "
                "limit 0 , {}").format(count)

        cursor.execute(query)
       
        v = 0
        for x in cursor.fetchall():
            print "{}:{} ({}) {}{}".format(v, x[2], x[3], x[0], x[1])
            v += 1 
        cursor.close()
        

    def run(self):
        self.build()
        cursor = self.cnx.cursor()
       

        query = ("INSERT INTO filter_prevalence(unique_file_id, count, num_systems, os_id) "
                 "SELECT  t.unique_file_id, COUNT(unique_file_id)as count, t.num_systems, t.os_idd from "
                 "(SELECT DISTINCT unique_file_id, media_source.id as src, s.os_idd, num_systems "
                 "from file_metadata LEFT JOIN media_source ON (file_metadata.source_id = media_source.id) "
                 "LEFT JOIN( select os.id as os_idd, os.name as os, COUNT(os.name) as num_systems "     
                 "from os LEFT JOIN media_source ON(os.id = media_source.os_id) GROUP BY os.name ) s "              
                 "ON (s.os_idd = file_metadata.os_id)) t "
                 "GROUP BY t.os_idd, t.unique_file_id;")

        cursor.execute(query)
        
      
        cursor.execute("UPDATE filter_prevalence SET average =  (SELECT count/num_systems)")
        cursor.execute("UPDATE filter_prevalence SET score = (SELECT IF(num_systems < 3, average * .3, average))")
        self.cnx.commit()

        query  = ("INSERT INTO dir_prevalence (unique_path_id, score) "
                "(SELECT path_id, avg(average) from "
                "(SELECT DISTINCT unique_path.id as path_id, file_metadata.unique_file_id, average, unique_path.full_path as path " 
                " from unique_path LEFT JOIN file_metadata "
                "ON (unique_path.id = unique_path_id) LEFT JOIN filter_prevalence "
                "ON (file_metadata.unique_file_id = filter_prevalence.unique_file_id)) b  GROUP BY path_id, path)")
                
        query = ("INSERT INTO dir_prevalence (unique_path_id, score) "
            "SELECT unique_path.id, avg(average) "
            "FROM unique_path "
            "LEFT JOIN file_metadata "
            "ON unique_path.id = unique_path_id "
            "LEFT JOIN filter_prevalence "
            "ON file_metadata.unique_file_id = filter_prevalence.unique_file_id AND file_metadata.os_id = filter_prevalence.os_id "
            "GROUP BY unique_path.id;")

        cursor.execute(query)
        
        self.cnx.commit()
        cursor.close()

    def update(self):
        pass

    def table_exists(self):
        cursor = self.cnx.cursor()
        result = None
        try:
            cursor.execute("select COUNT(id) from filter_prevalence")
            result = cursor.fetchone()
            cursor.close()
        except Exception as err:
            pass

       
        if(result == None or result[0] == 0):
            return False
        else: 
            return True



    def build(self):
        
        cursor = self.cnx.cursor() 
        cursor.execute("DROP TABLE IF EXISTS filter_prevalence")
        cursor.execute("DROP TABLE IF EXISTS dir_prevalence")
        print "Building filter staging tables"    
        self.cnx.commit()
        query = ("CREATE TABLE IF NOT EXISTS filter_prevalence ( "
                "id BIGINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                "unique_file_id BIGINT UNSIGNED NOT NULL, "
                "score DOUBLE NOT NULL DEFAULT .5, "
                "average DOUBLE NOT NULL DEFAULT .5,"
                "count INT NOT NULL DEFAULT 0,"
                "num_systems INT NOT NULL DEFAULT 0,"
                "os_id INT UNSIGNED NOT NULL, " 
                "PRIMARY KEY(id), "
                "INDEX fk_unique_file_idx (unique_file_id), "
                "INDEX fk_os_id_idx (os_id),"
                "INDEX score_idx USING BTREE (score ASC), "
                "CONSTRAINT fk_unique_file_idx FOREIGN KEY(unique_file_id) "
                "REFERENCES unique_file (id) "
                "ON DELETE NO ACTION ON UPDATE NO ACTION,"
                "CONSTRAINT fk_os_id_idx FOREIGN KEY(os_id) "
                "REFERENCES os (id) "
                "ON DELETE NO ACTION ON UPDATE NO ACTION "
                ") ENGINE = InnoDB;")
      
        cursor.execute(query)
        #TODO: add back the constraint once we match parent ids with file ids
        query = ("CREATE TABLE IF NOT EXISTS dir_prevalence ( "
                "directory_id BIGINT NOT NULL, "
                "score DOUBLE NOT NULL DEFAULT .5,"
                "unique_path_id INT UNSIGNED NOT NULL,"
                "PRIMARY KEY(directory_id),"
                "INDEX directory_id_idx USING BTREE (directory_id),"
                "INDEX fk_unique_path_id_idx(unique_path_id),"
                "CONSTRAINT fk_unique_path_id_idx FOREIGN KEY(unique_path_id) "
                "REFERENCES unique_path (id) "
                "ON DELETE NO ACTION ON UPDATE NO ACTION"
                ") ENGINE = InnoDB"
                )
        
        cursor.execute(query)
        self.cnx.commit()
        cursor.close()
