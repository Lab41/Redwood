#Association-Based Data Reduction (REDWOOD)

This project implements statistical methods to assist in identifying anomalous files from a larger data set.  

##Quick Setup
The instructions that follow should get you up an running quickly.  Currently Redwood is setup to run on OS X and Linux, though Windows may work too, though it hasn't been tested.

#### Stuff to Download
1. Python 2.7
2. Various Python packages
  * SciPy
  * MatPlotLib
  * MySQLdb
3. MySQL Client for your client OS
4. MySQL Server for the server hosting the DB

#### Prep the Database
Redwood uses a MySQL database to store metadata. In order to use Redwood, you will need to first set up your own MySQL DB, then run the following two SQL scripts to create the required tables and subroutines.

```
mysql -uyour_db_user -pyour_password -hyour_host -Dyour_database < sql/create_redwood_db.sql
mysql -uyour_db_user -pyour_password -hyour_host -Dyour_database < sql/create_redwood_sp.sql
```

#### Create a config

```
[mysqld]
database:your_db_name
host:your_host
username:your_username
password:your_password
```

## Run Redwood

#### Using PIP or Souce



### Using the Sample Shell

```
#append to the python path the Redwood directory
export PYTHONPATH=/path/to/Redwood
#from the Redwood directory run
python sample_shell/run.py /path/to/config
```

##Documentation
from the root project directory, run the following
```
sphinx-apidoc -o docs redwood -F
pushd docs
make html
make man
popd
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

Redwood uses a series of filters that run statistical methods on the data. All filters are plugins into the Redwood architecture.  A filter must be added to the global __plugins__ list in order for Redwood to recognize this.  To add it to the list, you can either save it to a directory "Filters" in the current directory, save it to redwood/filters, or finally use the API to add it programatically.  These filters are the core of how Redwood assigns scores to individual files. To add a filter to Redwood, extend the RedwoodFilter class in redwood/filters/redwood_filter.py as shown below: 

```
class YourFilterName(RedwoodFilter)

    def __init__(self):
        self.name = "YourFilterName"
        self.score_table = "YourScoreTableName"
        self.cnx
    def usage(self):
        print "Your usage statement"

    def update(self, source_name):
        #code to update all filter tables with source_name data

    #discovery functions
    def discover_your_discover_func0(self, arg0, ..., argN):
        your code
    ...
    def discover_your discover_funcM(self, arg0, ..., argN):
        your code

```

Notes about filter creation
*  The update function must produce (or update if not exists) a table called self.score_table with two columns (id, score) where the id is the unique_file.id of the the given file and the score is the calculated score
*  The self.cnx instance variable must be set prior to running any of the functions of the filter. The self.cnx is a mysql connection object

##Optimizing MySQL Notes
bulk_insert_buffer_size: 8G

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/Lab41/redwood/trend.png)](https://bitdeli.com/free "Bitdeli Badge")
