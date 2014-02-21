import redwood.helpers.core as core

notable_filetypes = ['exe', 'dll', 'sys', 'doc', 'docx', 'pdf', 'zip', 'rar', 'js', 'unknown', 'dotnetexe', 'dotnetdll']

from redwood.filters.redwood_filter import RedwoodFilter

class FileTypes(RedwoodFilter):
    def __init__(self, cnx=None):
        self.name = "File_Types"
        self.score_table = "ft_scores"
        self.cnx = cnx         

    def usage(self):
        """
        Prints the usage statement
        """

    def update(self, source):
        """
        Updates the scores of the ft_scores table with the new data from the inputted source

        :param source: identifier for the source to be updated
        """

        print "[+] File Type Filter running on {} ".format(source)

        #creates the basic tables if they do not exist
        self.build()

        cursor = self.cnx.cursor()
        
        src_info = core.get_source_info(self.cnx, source)
        
        if src_info is None:
            print "Error: Source {} not found".format(source)
            return

	query = """SELECT fmd.unique_file_id, fmd.file_type FROM media_source
	INNER JOIN file_metadata fmd
	ON fmd.source_id = media_source.id
	WHERE name = '{}'
	""".format(source)
        
	cursor.execute(query)
	results = cursor.fetchall()
	results_count = len(results)

	query = """SELECT file_type, COUNT(file_type) FROM file_metadata
	WHERE source_id = {}
	GROUP BY file_type""".format(src_info.source_id)
	cursor.execute(query)
	file_type_counts = cursor.fetchall()
	score_list = []

	for r in results:
	    new_data = []
	    new_data.append(r[0])
	    extension = r[1].strip()
	    #for f in notable_filetypes:
	    #    if extension in f:
	    #	    new_data.append(0)
	    #	else:
	    #	    new_data.append(1)
		#ext = [item for item in file_type_counts if r[1] in item]
		#score = float((ext[0][1])) / float(results_count)
	    #   score_list.append(new_data)
	    if extension in notable_filetypes:
		new_data.append(0)
	    else:
		new_data.append(1)
	    score_list.append(new_data)

	#print score_list
        query = """INSERT IGNORE INTO ft_scores (id, score) VALUES """

	for score in score_list:
	    query += "({},{}), ".format(score[0], score[1])

	query = query[:len(query) - 2]
	cursor.execute(query)
        self.cnx.commit()
        cursor.close()

    def clean(self):
        """
        Cleans all tables associated with this filter
        """
        cursor = self.cnx.cursor() 
        cursor.execute("DROP TABLE IF EXISTS ft_scores")
        self.cnx.commit()

    def build(self):
        """
        Builds all persistent tables associated with this filter
        """

        cursor = self.cnx.cursor()
       
        query = """
            CREATE TABLE IF NOT EXISTS `ft_scores` (
            id BIGINT unsigned NOT NULL,
            score double DEFAULT NULL,
            PRIMARY KEY(id),
            CONSTRAINT `fk_unique_file2_id` FOREIGN KEY (`id`) 
            REFERENCES `unique_file` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
                     ) ENGINE=InnoDB
        """
 
        cursor.execute(query)   
        self.cnx.commit()
        cursor.close()

    def run_survey(self, source_name):
	return
