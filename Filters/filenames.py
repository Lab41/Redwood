from redwood.filters.redwood_filter import RedwoodFilter
import redwood.helpers.core as core
import time
import os
import shutil

class FileNameFilter(RedwoodFilter):

    def __init__(self):
        self.name =  "FileNameFilter"
        self.score_table = "FileNameFilter_scores"

    def clean(self):
        """
        Cleans all tables associtaed witht his filter
        """
        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS FileNameFilter_scores")
        cursor.execute("DROP TABLE IF EXISTS FileNameFilter_unique_name")
        self.cnx.commit()
        cursor.close()

    def build(self):
        """
        Builds all persistent tables associated with this filter
        """
        cursor = self.cnx.cursor()
        query = """
            CREATE TABLE IF NOT EXISTS `FileNameFilter_scores` (
            id BIGINT unsigned NOT NULL,
            score double DEFAULT NULL,
            PRIMARY KEY(id),
            CONSTRAINT `FNF_unique_file1_id` FOREIGN KEY (`id`) 
            REFERENCES `unique_file` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
             ) ENGINE=InnoDB
        """
        cursor.execute(query)   
        self.cnx.commit()

        query = """
            CREATE TABLE IF NOT EXISTS FileNameFilter_unique_name (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            file_name VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
            unique_path_id INT(10) NOT NULL,
            count INT DEFAULT 1,
            PRIMARY KEY (id),
            UNIQUE INDEX file_path_idx USING BTREE (file_name ASC, unique_path_id),
            INDEX file_name_idx USING BTREE (file_name ASC)
        )  ENGINE=InnoDB;
        """
        cursor.execute(query)   
        self.cnx.commit()
        cursor.close()

    def update(self, source):
        print "[+] FileName Filter running on {} ".format(source)

        #creates the basic tables if they do not exist
        self.build()

        cursor = self.cnx.cursor()
        
        src_info = core.get_source_info(self.cnx, source)
        
        if src_info is None:
            print "Error: Source {} not found".format(source)
            return
            
        now = time.time()

#        self.cnx.autocommit(False)
        query = """
            INSERT INTO FileNameFilter_unique_name 
                (file_name, unique_path_id) 
                (SELECT file_name, unique_path_id FROM file_metadata WHERE file_name != "/" and source_id = {})
            ON DUPLICATE KEY UPDATE count = count + 1;
        """.format(src_info.source_id)
        cursor.execute(query)
#        self.cnx.autocommit(True)
        
        later = time.time()
        
        #print "Updated counts in {} secs\nUpdating Scores".format(later - now)

        cursor.execute("SELECT MAX(count) FROM FileNameFilter_unique_name")
        (max_count,) = cursor.fetchone()
        
        now = time.time()
        query = """
            INSERT INTO FileNameFilter_scores
                (id, score) 
                (
                    SELECT
                        fm.unique_file_id, MIN(fnfun.count / {})
                    FROM FileNameFilter_unique_name fnfun
                    LEFT JOIN file_metadata fm 
                        ON fnfun.file_name = fm.file_name
                        AND fnfun.unique_path_id = fm.unique_path_id
                    WHERE not isnull(fm.unique_file_id)
                    GROUP BY fm.unique_file_id
                )
            ON DUPLICATE KEY UPDATE score = score
            """.format(max_count)
        cursor.execute(query)
        self.cnx.commit()
        later = time.time()
        #print "Scores updated in {} secs".format(later - now)
        cursor.close()

    def discover_unique_names(self, source):
        """usage: unique_names source_name"""

        data = self.get_unique_names(source)
        
        if data is not None:
            for (file, dir) in data:
                print "Unique file %s %s" % (file, dir)
        

    def get_unique_names(self, source):
        """usage: unique_names source_name"""

        #creates the basic tables if they do not exist
        self.build()

        cursor = self.cnx.cursor()
        
        src_info = core.get_source_info(self.cnx, source)
        
        if src_info is None:
            print "Error: Source {} not found".format(source)
            return
            
        query = """
            SELECT fm.file_name, up.full_path 
            FROM file_metadata fm 
            LEFT JOIN FileNameFilter_unique_name fnfun
                ON fnfun.file_name = fm.file_name
                AND fnfun.unique_path_id = fm.unique_path_id
            LEFT JOIN unique_path up 
                ON up.id = fm.unique_path_id
            WHERE not isnull(fm.unique_file_id) 
                AND fnfun.count = 1
                AND fm.source_id = {}
        """.format(src_info.source_id)

        cursor.execute(query)
        data = cursor.fetchall()
                
        cursor.close()
        return data

    def run_survey(self, source_name):
        
        resources = "resources"
        survey_file = "survey.html"
        survey_dir = "survey_{}_{}".format(self.name, source_name)

        resource_dir = os.path.join(survey_dir, resources) 
        html_file = os.path.join(survey_dir, survey_file)
        
        try:
            shutil.rmtree(survey_dir)
        except:
            pass

        os.mkdir(survey_dir)
        os.mkdir(resource_dir)
        
        results = self.get_unique_names(source_name)
        
        with open(html_file, 'w') as f:

            f.write("""
            <html>
            <head>
            <link href="../../../resources/css/style.css" rel="stylesheet" type="text/css">
            </head>
            <body>
            <h2 class="redwood-title">FileNameFilter Snapshot</h2> 
            """)
            f.write("<h3 class=\"redwood-header\">One Timers in Directories</h3>")
            f.write("<table border=\"1\" id=\"redwood-table\">")
            f.write("<thead>")
            f.write("<tr><th class=\"rounded-head-left\">Parent Path</th><th class=\"rounded-head-right\">Filename</th></tr>")
            f.write("</thead><tbody>")
            i = 0
            lr = len(results)
            for (b,a) in results:
                if i == lr - 1:
                    f.write("</tbody><tfoot>")
                    f.write("<tr><td class=\"rounded-foot-left-light\">{}</td><td class=\"rounded-foot-right-light\">{}</td></tr></tfoot>".format(a, b))
                else:
                    f.write("<tr><td>{}</td><td>{}</td></tr>".format(a, b))
                i += 1
            f.write("</table>") 
            f.write("</body></html>")

        return survey_dir 
