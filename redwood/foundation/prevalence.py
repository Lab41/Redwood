
class PrevalenceAnalyzer():

    def __init__(self, cnx):
        self.cnx = cnx      

    def update(self, source_os_list):
        
        self.build()
        
        print "...Calculating prevalence of newly add sources"

        cursor = self.cnx.cursor()
        
        #iterate through each of the new sources, updating the prevalence table accordingly
        for pair in source_os_list:
            
            #TODO: make sure you don't add a source that already has been analyzed

            source_id = pair[0]
            os_id = pair[1]

            #will need to fetch the number of systems first for the given os
            query = """
                select COUNT(os.name) from os LEFT JOIN media_source ON(os.id = media_source.os_id) 
                where os.id = {} GROUP BY os.name 
            """.format(os_id)

            cursor.execute(query)
            num_systems = cursor.fetchone()[0]

            print "Num Systems: {}".format(num_systems)
            
            #this query will either insert a new entry into the table or update an existing ones
            #This will only get prevalence of files, NOT directories since all directories have the same zero
            #contents hash. We exclude dirs by checking file size != 0, though some dirs slip through with larger file sizes
            query =  """
                INSERT INTO global_file_prevalence(unique_file_id, count, num_systems, os_id)
                SELECT  t.unique_file_id, COUNT(unique_file_id) as count, t.num_systems, t.os_idd from
                (SELECT DISTINCT unique_file_id, media_source.id as src, s.os_idd, num_systems
                from file_metadata JOIN media_source ON (file_metadata.source_id = media_source.id)
                LEFT JOIN( select os.id as os_idd, os.name as os, COUNT(os.name) as num_systems     
                from os LEFT JOIN media_source ON(os.id = media_source.os_id) 
                WHERE os.id = {} GROUP BY os.name ) s              
                ON (s.os_idd = file_metadata.os_id) where media_source.id = {} AND file_metadata.size != 0) t 
                GROUP BY t.os_idd, t.unique_file_id
                ON DUPLICATE KEY UPDATE  count=count+1
            """.format(os_id, source_id)
            
            cursor.execute(query)
        
            #TODO: use a local variable for num_systems
            query = """
                UPDATE global_file_prevalence SET num_systems = {}, average =  (SELECT count/num_systems) where os_id = {}
            """.format(num_systems, os_id)
        
            cursor.execute(query)
            
            #get the prevalence of directories
            query = """
                INSERT INTO global_dir_prevalence (unique_path_id, count, num_systems, os_id)
                    SELECT unique_path.id as path_id, COUNT(file_metadata.id) as count, t.num_systems, file_metadata.os_id 
                    from unique_path LEFT JOIN file_metadata
                    ON file_metadata.unique_path_id = unique_path.id LEFT JOIN 
                    (SELECT os.id as os_i, COUNT(media_source.id) as num_systems from os 
                    LEFT JOIN media_source ON os.id = media_source.os_id 
                    GROUP BY os.id) as t ON (file_metadata.os_id = t.os_i) 
                    where file_metadata.file_name = '/' AND file_metadata.source_id = {} 
                    GROUP BY file_metadata.os_id, unique_path.id
                    ON DUPLICATE KEY UPDATE count=count+1
            """.format(source_id)

            cursor.execute(query)

            query = """
                UPDATE global_dir_prevalence SET num_systems = {}, average = (SELECT count/num_systems) where os_id = {}
            """.format(num_systems, os_id)

            self.cnx.commit()

        #TODO: There should be a better way for below code 
        print "Rebuilding the aggregated prevalence table for directories"
    
        cursor.execute("DROP TABLE IF EXISTS global_dir_combined_prevalence")

        self.cnx.commit()
        
        query = """
            CREATE TABLE IF NOT EXISTS global_dir_combined_prevalence (
            unique_path_id INT UNSIGNED NOT NULL,
            average DOUBLE NOT NULL DEFAULT .5,
            PRIMARY KEY(unique_path_id),
            CONSTRAINT fk_unique_path_idx3 FOREIGN KEY(unique_path_id)
            REFERENCES unique_path (id)
            ON DELETE NO ACTION ON UPDATE NO ACTION
            ) ENGINE = InnoDB;
        """

        cursor.execute(query)
        self.cnx.commit()

        query = """
            INSERT INTO global_dir_combined_prevalence
            SELECT unique_path_id, avg(average) FROM file_metadata 
            INNER JOIN global_file_prevalence ON file_metadata.unique_file_id = global_file_prevalence.unique_file_id 
                where file_metadata.file_name != '/' GROUP BY unique_path_id
        """
        
        cursor.execute(query)
        self.cnx.commit()

    def clean(self):
         
        cursor = self.cnx.cursor() 
        cursor.execute("DROP TABLE IF EXISTS global_file_prevalence")
        cursor.execute("DROP TABLE IF EXISTS global_dir_prevalence")
        cursor.execute("DROP TABLE IF EXISTS global_dir_combined_prevalence")
        self.cnx.commit()


    def build(self):
        
        print "Building the staging tables"
       
        cursor = self.cnx.cursor()

        query = """
            CREATE TABLE IF NOT EXISTS global_file_prevalence (
            unique_file_id BIGINT UNSIGNED NOT NULL,
            average DOUBLE NOT NULL DEFAULT .5,
            count INT NOT NULL DEFAULT 0,
            num_systems INT NOT NULL DEFAULT 0,
            os_id INT UNSIGNED NOT NULL,
            PRIMARY KEY(unique_file_id, os_id),
            INDEX fk_unique_file_idx1 (unique_file_id),
            INDEX fk_os_id_idx1 (os_id),
            CONSTRAINT fk_unique_file_idx1 FOREIGN KEY(unique_file_id)
            REFERENCES unique_file (id)
            ON DELETE NO ACTION ON UPDATE NO ACTION,
            CONSTRAINT fk_os_id_idx1 FOREIGN KEY(os_id)
            REFERENCES os (id)
            ON DELETE NO ACTION ON UPDATE NO ACTION
            ) ENGINE = InnoDB;
        """
      
        cursor.execute(query)
       

        query = """
            CREATE TABLE IF NOT EXISTS global_dir_prevalence (
            unique_path_id INT UNSIGNED NOT NULL,
            average DOUBLE NOT NULL DEFAULT .5,
            count INT NOT NULL DEFAULT 0,
            num_systems INT NOT NULL DEFAULT 0,
            os_id INT UNSIGNED NOT NULL,
            PRIMARY KEY(unique_path_id, os_id),
            INDEX fk_unique_path_idx1 (unique_path_id),
            INDEX fk_os_id_idx2 (os_id),
            CONSTRAINT fk_unique_path_idx2 FOREIGN KEY(unique_path_id)
            REFERENCES unique_path (id)
            ON DELETE NO ACTION ON UPDATE NO ACTION,
            CONSTRAINT fk_os_id_idx2 FOREIGN KEY(os_id)
            REFERENCES os (id)
            ON DELETE NO ACTION ON UPDATE NO ACTION
            ) ENGINE = InnoDB;
        """
      
        cursor.execute(query)
        
        self.cnx.commit()
        cursor.close()
