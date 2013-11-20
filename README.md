#Association-Based Data Reduction (REDWOOD)

This project implements statistical methods to assist in identifying anomalous files from a larger data set.  

##Documentation
from the root project directory, run the following
```
sphinx-apidoc -o docs redwood -F
pushd docs
make html
make man
popd
```

##Python Package Requirements

- MySQLdb - MySQL-Python Project provides MySQL Connector (http://mysql-python.sourceforge.net/)
- python 2.7
- SciPy - http://www.scipy.org/


##Setup

###Databases
First create the database

From the Redwood directory, run
```
mysql -uyour_db_user -pyour_password -hyour_host < sql/create_redwood_db.sql
mysql -uyour_db_user -pyour_password -hyour_host < sql/create_redwood_sp.sql
```

###Run Redwood
write a connection config
```
[mysqld]
database:your_db_name
host:your_host
username:your_username
password:your_password
```

Then, start the application, specifying the config you just created

```
python redwood.py /path/to/connection/config
```

###Obtain Data

The load_csv command expects that the fields be in the following order
```
sha1,dirname,basename,inode,device,permissions,user_owner,group_owner,last_accessed,last_modified,last_changed,inode_birth,user_flags,links_to_file,size
```

The script below will walk a hfs+ file system and (perhaps) other Unix/Linux file systems, collecting the relevant metadata using the stat command.  The output will be in the appropriate format for the load_csv command

```
echo "sha1,dirname,basename,inode,device,permissions,user_owner,group_owner,last_accessed,last_modified,last_changed,inode_birth,user_flags,links_to_file,size" > filewalk; sudo find / -type f -exec  sh -c 'A=$(shasum "$0" | cut -d" "  -f1-2 | tr -d " ") ; DIR="$(dirname "$0")/"; BASE=$(basename "$0"); B=$(stat -f  "%i,%d,%p,%Su,%Sg,%a,%m,%c,%B,%f,%l,%z" "$0") ;  echo $A,$DIR,$BASE,$B >> filewalk ; ' {} \;
```

##Plugin Architecture

Redwood uses a series of filters that run statistical methods on the data. These filters are the core of how Redwood assigns scores to individual files. To add a filter to Redwood, extend the RedwoodFilter class as shown below: 

```
class YourFilterName(RedwoodFilter)

    def __init__(self):
        self.name = "YourFilterName"
        self.score_table = "YourScoreTableName"

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

    def run(self, cnx):
        your code

```

##Optimizing MySQL Notes
bulk_insert_buffer_size: 8G

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/Lab41/redwood/trend.png)](https://bitdeli.com/free "Bitdeli Badge")
