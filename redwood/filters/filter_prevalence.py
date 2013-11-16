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

        self.clean()
        self.build()
        cursor = self.cnx.cursor()
        
        cursor.execute(query)

        #initialize the fp_scores table. The score is the average unless we don't have enough systems
        #which is less than 3 for now, in which case we just set the score to .5
        query = """
            INSERT INTO  fp_scores(id, score)
            SELECT unique_file_id, IF(num_systems < 3, .5, average) FROM global_file_prevalence
        """
        
        cursor.execute(query)
        self.cnx.commit()
        

        #adjustment for low outliers in high prevalent directories... This could probably better be done with taking the std dev of each
        #dir, but his will have to work for now. beware duplication here... TODO
        query = """
            UPDATE  global_file_prevalence left join file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
            LEFT JOIN global_dir_prevalence on file_metadata.unique_path_id = global_dir_prevalence.unique_path_id
            LEFT JOIN global_dir_combined_prevalence on file_metadata.unique_path_id = global_dir_combined_prevalence.unique_path_id 
            LEFT JOIN fp_scores ON fp_scores.id = global_file_prevalence.unique_file_id
            SET fp_scores.score = fp_scores.score * .5 
            where global_file_prevalence.num_systems > 2 and global_dir_combined_prevalence.average - global_file_prevalence.average > .6
        """
       
        cursor.execute(query)
        self.cnx.commit()

        #adjustments for low prevalent scored directories which occur often... hopefully this will exclude the caches
        query = """
            UPDATE file_metadata 
            LEFT JOIN global_dir_prevalence ON file_metadata.unique_path_id = global_dir_prevalence.unique_path_id 
            LEFT JOIN global_dir_combined_prevalence ON global_dir_combined_prevalence.unique_path_id = global_dir_prevalence.unique_path_id
            LEFT JOIN fp_scores ON file_metadata.unique_file_id = fp_scores.id
            SET fp_scores.score = (1 - fp_scores.score) * .25 + fp_scores.score
            where global_dir_prevalence.average > .8 AND global_dir_combined_prevalence.average < .5
        """
        
        cursor.exceute(query)
        self.cnx.commit()
        cursor.close()

    def update(self, source):
        
        cursor = self.cnx.cursor()

        cursor.execute("SELECT id, os_id from media_source where name = '{}'".format(source))
        r = cursor.fetchone()

        if r is None:
            print "Error: Source with name \"{}\" does not exist".format(source)
            return

        source_id = r[0]
        os_id = r[1]

        print "Evaluating {} ".format(source)
        
        #initial insert
        query = """
            INSERT INTO  fp_scores(id, score)
            SELECT global_file_prevalence.unique_file_id, IF(num_systems < 3, .5, average) 
            FROM global_file_prevalence JOIN file_metadata
            ON file_metadata.unique_file_id = global_file_prevalence.unique_file_id
            where file_metadata.source_id = 4
            ON DUPLICATE KEY UPDATE score = IF(num_systems < 3, .5, average)
        """

        cursor.execute(query)
        self.cnx.commit()
        
        #adjustment for low outliers in high prevalent directories... This could probably better be done with taking the std dev of each
        #dir, but his will have to work for now.  
        query = """
            UPDATE  global_file_prevalence left join file_metadata ON global_file_prevalence.unique_file_id = file_metadata.unique_file_id
            LEFT JOIN global_dir_prevalence on file_metadata.unique_path_id = global_dir_prevalence.unique_path_id
            LEFT JOIN global_dir_combined_prevalence on file_metadata.unique_path_id = global_dir_combined_prevalence.unique_path_id 
            LEFT JOIN fp_scores ON fp_scores.id = global_file_prevalence.unique_file_id
            SET fp_scores.score = fp_scores.score * .5 
            where file_metadata.source_id = {} AND global_file_prevalence.count = 1 and global_file_prevalence.num_systems > 2 
            and global_dir_combined_prevalence.average - global_file_prevalence.average > .6
        """.format(source_id)
       
        cursor.execute(query)
        self.cnx.commit()

        #adjustments for low prevalent scored directories which occur often... hopefully this will exclude the caches
        query = """
            UPDATE file_metadata 
            LEFT JOIN global_dir_prevalence ON file_metadata.unique_path_id = global_dir_prevalence.unique_path_id 
            LEFT JOIN global_dir_combined_prevalence ON global_dir_combined_prevalence.unique_path_id = global_dir_prevalence.unique_path_id
            LEFT JOIN fp_scores ON file_metadata.unique_file_id = fp_scores.id
            SET fp_scores.score = (1 - fp_scores.score) * .25 + fp_scores.score
            where file_metadata.source_id = {} AND global_dir_prevalence.average > .8 AND global_dir_combined_prevalence.average < .5
        """.format(source_id)
        
        cursor.execute(query)
        self.cnx.commit()
        cursor.close()

    def clean(self):
         
        cursor = self.cnx.cursor() 
        cursor.execute("DROP TABLE IF EXISTS fp_scores")
        self.cnx.commit()

    def build(self):
        
        query = """
            CREATE TABLE IF NOT EXISTS `fp_scores` (
            id BIGINT unsigned NOT NULL,
            score double DEFAULT NULL,
            PRIMARY KEY(id),
            CONSTRAINT `fk_unique_file1_id` FOREIGN KEY (`id`) 
            REFERENCES `unique_file` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
                     ) ENGINE=InnoDB
        """
 
        cursor.execute(query)
        
        self.cnx.commit()
        cursor.close()
