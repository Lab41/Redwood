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


    def discover_directory_prev(self, refresh, count, direction):
        
        cursor = self.cnx.cursor()

        if refresh is 'true':
            query = """
                INSERT into dir_prevalence(unique_path_id, avg_score, count)
                    SELECT unique_path_id, average_score, t2.cnt from (SELECT unique_path_id, avg(average) as average_score from 
                        (SELECT avg(score) as average, unique_path_id  FROM file_metadata 
                            INNER JOIN filter_prevalence ON file_metadata.unique_file_id = filter_prevalence.unique_file_id 
                            GROUP BY unique_path_id) as t 
                        GROUP BY unique_path_id) as t0 inner join
                        (SELECT COUNT(id) as cnt, unique_path_id as path_id 
                    FROM file_metadata where file_name = '/' GROUP BY unique_path_id) as t2 on unique_path_id = t2.path_id
            """

            cursor.execute(query)


        if direction is 'high':
            pass
        elif direction is 'low':
            pass
        else:
            print "Direction must be \"high\" or \"low\""
            return



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

        query = """
                INSERT into dir_prevalence(unique_path_id, avg_score, count)
                    SELECT unique_path_id, average_score, t2.cnt from (SELECT unique_path_id, avg(average) as average_score from 
                        (SELECT avg(score) as average, unique_path_id  FROM file_metadata 
                            INNER JOIN filter_prevalence ON file_metadata.unique_file_id = filter_prevalence.unique_file_id 
                            GROUP BY unique_path_id) as t 
                        GROUP BY unique_path_id) as t0 inner join
                        (SELECT COUNT(id) as cnt, unique_path_id as path_id 
                    FROM file_metadata where file_name = '/' GROUP BY unique_path_id) as t2 on unique_path_id = t2.path_id
        """

        cursor.execute(query)
        self.cnx.commit()

        cursor.execute("DROP TABLE IF EXISTS fp_scores")

        query = ("""CREATE TABLE IF NOT EXISTS `fp_scores` (
                `id` bigint(20) unsigned NOT NULL,
                `score` double DEFAULT NULL,
                KEY `fk_unique_file1_id` (`id`),
                CONSTRAINT `fk_unique_file1_id` FOREIGN KEY (`id`) 
                REFERENCES `unique_file` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
                        ) ENGINE=InnoDB""")

        cursor.execute(query)

        query = """
        INSERT INTO  fp_scores(id, score)
            SELECT unique_file_id, score FROM filter_prevalence
        """
        
        cursor.execute(query)

        cursor.close()




    def update(self, source):
        
        self.build()
        #this will not be thread safe... an update requires that no others write to the prevalence tables
        
        cursor = self.cnx.cursor()

        cursor.execute("SELECT id, os_id from media_source where name = '{}'".format(source))
        r = cursor.fetchone()

        if r is None:
            print "Error: Source with name \"{}\" does not exist".format(source)
            return

        source_id = r[0]
        os_id = r[1]


        #check if we have already analyzed this source
        cursor.execute("""SELECT count(*) from fp_analyzed_sources where id = '{}'""".format(source_id))
        found = cursor.fetchone()

        if found[0] > 0:
            print "already analyzed source {}".format(source)
            return
        else:
            query = "insert into fp_analyzed_sources (id, name) VALUES('{}','{}')".format(source_id, source)
            cursor.execute(query)
            self.cnx.commit()

        print "Evaluating {} ".format(source)
 
        #will need to fetch the number of systems first for the given os
        query = """
            select COUNT(os.name) from os LEFT JOIN media_source ON(os.id = media_source.os_id) 
            where os.id = {} GROUP BY os.name 
        """.format(os_id)

        cursor.execute(query)
        num_systems = cursor.fetchone()[0]

        print "Num Systems: {}".format(num_systems)

        #if the os has never been seen by the filter, then just insert straight up
        query =  """
            INSERT INTO filter_prevalence(unique_file_id, count, num_systems, os_id)
            SELECT  t.unique_file_id, COUNT(unique_file_id) as count, t.num_systems, t.os_idd from
            (SELECT DISTINCT unique_file_id, media_source.id as src, s.os_idd, num_systems
            from file_metadata JOIN media_source ON (file_metadata.source_id = media_source.id)
            LEFT JOIN( select os.id as os_idd, os.name as os, COUNT(os.name) as num_systems     
            from os LEFT JOIN media_source ON(os.id = media_source.os_id) where os.id = {} GROUP BY os.name ) s              
            ON (s.os_idd = file_metadata.os_id) where media_source.id = {}) t GROUP BY t.os_idd, t.unique_file_id
	    ON DUPLICATE KEY UPDATE  filter_prevalence.num_systems={}, count=count+1
        """.format(os_id, source_id, num_systems)
        
        cursor.execute(query)

        query = """
            UPDATE filter_prevalence SET average =  (SELECT count/num_systems), 
            score = (SELECT IF(num_systems < 3, average * .3, average))
        """
        
        cursor.excecute(query)
        self.cnx.commit()


        cursor.execute(query)
        
        self.cnx.commit()
        #=========dir_prevalence table=================
        #update just those rows that have a similar unique_path_id
         
        cursor.close()

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

    def clean(self):
         
        cursor = self.cnx.cursor() 
        cursor.execute("DROP TABLE IF EXISTS filter_prevalence")
        cursor.execute("DROP TABLE IF EXISTS dir_prevalence")
        cursor.execute("DROP TABLE IF EXISTS fp_analyzed_sources")
        self.cnx.commit()


    def build(self):
        
        print "Building the staging tables"
       
        cursor = self.cnx.cursor()

        query = """
            CREATE TABLE IF NOT EXISTS fp_analyzed_sources (
            id INT UNSIGNED NOT NULL,
            name VARCHAR(45) NOT NULL,
            PRIMARY KEY(id),
            CONSTRAINT fk_media_source_id1 FOREIGN KEY (id)
            REFERENCES media_source(id)
            ON DELETE NO ACTION ON UPDATE NO ACTION
            ) ENGINE=InnoDB;
        """
        
        cursor.execute(query)
        
        query = """
            CREATE TABLE IF NOT EXISTS filter_prevalence (
            unique_file_id BIGINT UNSIGNED NOT NULL,
            score DOUBLE NOT NULL DEFAULT .5,
            average DOUBLE NOT NULL DEFAULT .5,
            count INT NOT NULL DEFAULT 0,
            num_systems INT NOT NULL DEFAULT 0,
            os_id INT UNSIGNED NOT NULL,
            PRIMARY KEY(unique_file_id, os_id),
            INDEX fk_unique_file_idx (unique_file_id),
            INDEX fk_os_id_idx (os_id),
            INDEX score_idx USING BTREE (score ASC),
            CONSTRAINT fk_unique_file_idx FOREIGN KEY(unique_file_id)
            REFERENCES unique_file (id)
            ON DELETE NO ACTION ON UPDATE NO ACTION,
            CONSTRAINT fk_os_id_idx FOREIGN KEY(os_id)
            REFERENCES os (id)
            ON DELETE NO ACTION ON UPDATE NO ACTION
            ) ENGINE = InnoDB;
        """
      
        cursor.execute(query)
        
        #gets the average prevalence for a unique path based on the average
        #of the individual dir prevalence across all systems
        query = """
            CREATE TABLE IF NOT EXISTS dir_prevalence (
            unique_path_id INT UNSIGNED NOT NULL,
            avg_score DOUBLE NOT NULL DEFAULT .5,
	    count INT NOT NULL DEFAULT 0,                
	    PRIMARY KEY(unique_path_id),
            INDEX fk_unique_path_id_idx(unique_path_id),
            CONSTRAINT fk_unique_path_id_idx FOREIGN KEY(unique_path_id)
            REFERENCES unique_path (id)
            ON DELETE NO ACTION ON UPDATE NO ACTION
            ) ENGINE = InnoDB
        """
        
        cursor.execute(query)
        self.cnx.commit()
        cursor.close()
