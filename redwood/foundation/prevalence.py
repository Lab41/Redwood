#!/usr/bin/env python
#
# Copyright (c) 2013 In-Q-Tel, Inc/Lab41, All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
The PrevalenceAnalyzer is a core component of Redwood for determining prevalence
analytics that can then be made available to all filters. 

Created on 19 October 2013
@author: Lab41
"""


class PrevalenceAnalyzer():

    def __init__(self, cnx):
        self.cnx = cnx      

    def update(self, sources):
        """
        Analyzes all sources from the source_os_list, storing results in the global tables
        for prevalence

        :param source_os_list: a list of tuples containing information abou the sources. The tuple
        contains (source_id, source_name, os_id)
        """
        self.build()
        
        print "[+] Conducting global analysis for prevalence"

        cursor = self.cnx.cursor()
        
        #iterate through each of the new sources, updating the prevalence table accordingly
        for source in sources:
            
            #will need to fetch the number of systems first for the given os
            query = """
                select COUNT(os.name) from os LEFT JOIN media_source ON(os.id = media_source.os_id) 
                where os.id = {} GROUP BY os.name 
            """.format(source.os_id)

            cursor.execute(query)
            num_systems = cursor.fetchone()[0]

            #this query will either insert a new entry into the table or update an existing ones
            #This will only get prevalence of files, NOT directories since all directories have the same zero
            #contents hash. We exclude dirs by checking file size != 0, though some dirs slip through with larger file sizes
            query = """
                INSERT INTO global_file_prevalence(unique_file_id, count, num_systems, os_id)
                SELECT  t.unique_file_id, COUNT(unique_file_id) as count, t.num_systems, t.os_idd from
                (SELECT DISTINCT unique_file_id, media_source.id as src, s.os_idd, num_systems
                from file_metadata JOIN media_source ON (file_metadata.source_id = media_source.id)
                LEFT JOIN( select os.id as os_idd, os.name as os, COUNT(os.name) as num_systems     
                from os LEFT JOIN media_source ON(os.id = media_source.os_id) 
                WHERE os.id = {}  GROUP BY os.name ) s              
                ON (s.os_idd = file_metadata.os_id) where media_source.id = {} AND file_metadata.unique_file_id is not null) t 
                GROUP BY t.os_idd, t.unique_file_id
                ON DUPLICATE KEY UPDATE  count=count+1
            """.format(source.os_id, source.source_id)
            
            cursor.execute(query)
        
            #TODO: use a local variable for num_systems
            query = """
                UPDATE global_file_prevalence SET num_systems = {}, average =  (SELECT count/num_systems) where os_id = {}
            """.format(num_systems, source.os_id)
        
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
            """.format(source.source_id)

            cursor.execute(query)

            query = """
                UPDATE global_dir_prevalence SET num_systems = {}, average = (SELECT count/num_systems) where os_id = {}
            """.format(num_systems, source.os_id)

            cursor.execute(query)

            self.cnx.commit()

        #TODO: There should be a better way for below code 
        print "[+] Rebuilding the aggregated prevalence table for directories"
    
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
        """
        Removes all required tables
        """

        cursor = self.cnx.cursor() 
        cursor.execute("DROP TABLE IF EXISTS global_file_prevalence")
        cursor.execute("DROP TABLE IF EXISTS global_dir_prevalence")
        cursor.execute("DROP TABLE IF EXISTS global_dir_combined_prevalence")
        self.cnx.commit()


    def build(self):
        """
        Builds all required tables
        """
        cursor = self.cnx.cursor()

        query = """
            CREATE TABLE IF NOT EXISTS global_file_prevalence (
            unique_file_id BIGINT UNSIGNED NOT NULL,
            average DOUBLE NOT NULL DEFAULT .5,
            count INT NOT NULL DEFAULT 0,
            num_systems INT NOT NULL DEFAULT 0,
            os_id INT UNSIGNED NOT NULL,
            PRIMARY KEY(unique_file_id, os_id),
            INDEX idx_fp_average (average) USING BTREE,                   
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
