#Association-Based Data Reduction (REDWOOD)

<i>Finding the Tree in the Forest</i>

![Redwood](https://raw.github.com/Lab41/Redwood/master/images/logo/redwood_logo.png "Redwood")


<p><b>Redwood is a Python framework intended to identify anomalous files through analyzing the file metadata of a collection of media</b>. Each file analyzed is assigned a score that signals its reputation relative to other files in the system --the lower a reputation score, the more likely that a file is anomalous.  The final reputation score of a given file is based on an aggegation of scores assigned to it by modules that we call "Filters".</p>
<p>A Filter is a plugin whose functionality is only limited by the creativity of the developer.  Redwood can support any number of Filters, so long as a Filter extends the RedwoodFilter class and produces a table assigning a reputation score to each unique file in the system.  Much of the Redwood framework is aimed at making the process of adding new Filters to the system as frictionless as possible (see the Filter section below for more information).</p>   
<p>In addition to the Filters, Redwood also provides an effective data model for analyzing and storing file metadata, an API for interacting with that data, a simple shell for executing Redwood commands, and two example Filters (a "prevalence" Filter and a "locality uniqueness" Filter.  Though sample Filters are included in the project, ultimately the effectiveness of Redwood will be based on the Filters that you write for the particular anomaly that you are looking for. To that end, Redwood is nothing more than a simple framework for connecting Filters to a well-formed data model.</p> 

##Quick Setup
The instructions that follow should get you up and running quickly.  Redwood has been tested on  OS X and Linux. Windows will likely work with a few changes.

#### Stuff to Download
1. Python 2.7
2. Python packages
  * SciPy (Matplotlib, MqSQLdb)
3. MySQL Client for your client OS
4. MySQL Server for the server hosting the DB

#### Prep the Database
Redwood uses a MySQL database to store metadata. In order to use Redwood, you will need to first set up your own MySQL DB, then run the following two SQL scripts to create the required tables and subroutines.

```bash
mysql -uyour_db_user -pyour_password -hyour_host -Dyour_database < sql/create_redwood_db.sql
mysql -uyour_db_user -pyour_password -hyour_host -Dyour_database < sql/create_redwood_sp.sql
```

#### Create a config

Create a file containing the following configuration information specific to your database

```
[mysqld]
database:your_db_name
host:your_host
username:your_username
password:your_password
```

## Run Redwood

There are two ways that you can run Redwood.  If you just want to play with the tool, and maybe create a couple of filters, the "Redwood Shell" method is probably the best choice.  If you want to make modifications to the core package and or create your own UI, then you probably want to use the API.  Examples of how to do both are below:

#### Using the Redwood Shell

```bash
#append to the python path the Redwood directory
export PYTHONPATH=/path/to/Redwood
#from the Redwood directory run
python bin/redwood /path/to/config
```

#### Using the API to create your Application
<i>This is a brief example of how to use the API to load a media source into the database and then run specific filter functions on that source</i>

```python
import redwood.connection.connect as connect
import redwood.io.csv_importer as loader
import redwood.helpers.core as core

#connect to the database
cnx = connect.connect_with_config("my_db.cfg")

#load a csv to the database
loader.run(cnx,"directory_containing_csv_data_pulls", false)

core.import_filters("./Filters", cnx)

#grab instances of two specific filters
fp = core.get_filter_by_name("prevalence")
lu = core.get_filter_by_name("locality_uniqueness")

#generate a histogram to see distribution of files for that source
fp.discover_histogram_by_source("some_source")

#run a survey for a particular source
fp.run_survey("some_source")
```


##Documentation
from the root project directory, run the following
```bash
sphinx-apidoc -o docs redwood -F; pushd docs; make html; make man; popd
```

###Data

Redwood currently only loads data from a CSV file with the fields below. Information about these fields can typically be found in a stat

|Field Name | Field Description|
|-----------|------------------|
|file_id| Unique id of the file |
|parent_id| file_id of the parent |
|dirname| path excluding filename |
|basename| filename |
|hash| Sha1 of file contents |
|fs_id| Inode of linux or non-linux equivalent |
|device| Device Node identifier |
|permissions| Permission of the file |
|uid| User owner of the file |
|gid| Group owner of the file |
|size| Size in bytes |
|create_time | file create in  seconds from epoch | 
|access_time| file last accessed in seconds from epoch |
|mod_time| file modification in seconds from epoch |
|metadata_change_time| file change in seconds from epoch |
|user_flags| user flags | 
|links| links to the file |
|disk_offset| disk offset |
|entropy| entropy of the file |
|file_content_status|file content status|
|extensions| file extension if available |
|file_type| file type if auto discovered |


The **sql/filewalk.py** script will walk a hfs+ file system and (perhaps) other Unix/Linux file systems, collecting the relevant metadata using the stat command.  The output will be in the appropriate format for the load_csv command. Note, this script has been optimized for Linux/OS X.  It will not work on a Windows system... updates welcome)



##Redwood Architecture

Redwood is composed of 5 core engines, all backed by a MySQL DB 

1. Ingestion Engine
  - The ingestion engine is responsible for importing data into the datastore from a metadata source file (currently only supporting csv). 
2. Global Analytics Engine
  - The Global Analytics Engine is responsible for performing analytics on a global scale against all metadata and then providing those results to all filters for subsequent computation in the form of queriable tables.  This engine typically conducts time intensive queries that you only want to perform once per new source.  Currently, the only Global Analytics Engine is the "Prevalence" analyzer.  This is not to be confused with the prevalence filter which leverages the tables produced by the the Prevalence analyzer. 
3. Filter Engine
  - The Filter Engine has two main responsiblities. The first is to create a table for the reputation scores that it has calculated for each unique file in the the database.  The second is to optionally provide a series of "Discovery" functions that are associated with the filter scoring yet can be used independently by the end user or developer to discover in more detail why a file has a paticular score. For more information, please refer to the "All About Filters" section.
4. Aggregation Engine
  - The Aggregation Engine is responsible for two main duties (1)  aggregating the scores of each filter into a single reputation score based on some aggregation algorithm (2) freezing global reputation scores if the engine deems them as either definitely high or low reputations
5. Reporting Engine
  - The Reporting Engine is responsible for generating a comprehensive report highlighting user specified information about the data. 


##All About Filters

####Summary
Filters are the foundation of file scoring in Redwood. A Filter's central purpose is to create a score for each unique file in the system.  After Redwood runs all the filters, each unique file should have a score from each filter.  It is then that Redwood is responsible for combining these scores using an aggregation function such that each unique file has only a single score in the unique file table.  Keep in mind that numerous filters can exist in a Redwood project.<br>
In addition to generating a score for each file, a Filter can optionally create one or more "Discovery" functions.  A Discovery function is a function that allows the user of the Filter to explore the data beyond just deriving a score. It is common for a Discovery function to also be used in the calculations for file scoring -- the Redwood model just provides a structured way for the developer to make that function available to the end user. 

####Writing your own Filter
Your filter should inherit from the base class RedwoodFilter in redwood.filters.redwood_filter. You must override those functions that raise a "NotImplementedError".  To assist in writing your own filter, look at the sample filters (locality_uniqueness and file_prevalence) in the Filters directory. 

- If you are using the Redwood Shell, any Filter placed in the Filters directory will be automatically imported into the application. 
- All discovery functions should be preceded by "discover_" in their name so that during introspection a developer knows which functions are intended for discovery
- A Filter is free to create any tables in the database. This can become necessary for efficiently calculating the reputation scores
- The update function must produce (or update if not exists) a table called self.score_table with two columns (id, score) where the id is the unique_file.id of the the given file and the score is the calculated score
- The self.cnx instance variable must be set prior to running any of the functions of the filter. The self.cnx is a mysql connection object. Redwood will set the cnx instance if you use its import functions.


```python
class YourFilterName(RedwoodFilter)

    def __init__(self):
        self.name = "YourFilterName"
        self.score_table = "YourScoreTableName"
        self.cnx
    def usage(self):
        print "Your usage statement"

    def update(self, source_name):
        #code to update all filter tables with source_name data
    
    #survey function
    def run_survey(source):
        your code
   
    #build
    def build():
        your code

    #clean
    def clean(self)
        your code

    #discovery functions
    def discover_your_discover_func0(self, arg0, ..., argN):
        your code
    ...
    def discover_your discover_funcM(self, arg0, ..., argN):
        your code

```

##Screen Shots 
<i>Sceenshot of the Sample Shell</i><br>
![Shell](https://raw.github.com/Lab41/Redwood/master/images/redwood_0.png "Redwood Shell")
<br><i>Sceenshot of the Filter Options</i><br>
![Shell](https://raw.github.com/Lab41/Redwood/master/images/discovery.png "Filter Options")
<br><i>Sceenshot of the File Distribution discovery function for Filter Prevalence</i><br>
![Shell](https://raw.github.com/Lab41/Redwood/master/images/histogram0.png "Prevalence Filter file distribution")
<br><i>Sceenshot of the discovery function for Locality Uniqueness</i><br>
![Clustering](https://raw.github.com/Lab41/Redwood/master/images/clustering.png "Locality Uniquenss Clustering")





##Optimizing MySQL Notes
bulk_insert_buffer_size: 8G

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/Lab41/redwood/trend.png)](https://bitdeli.com/free "Bitdeli Badge")
