import sys
import os
import getopt
import redwood.connect_helper as conn
import redwood.io.csv_importer as csv_load



def usage():
    print """
Usage
--------------------
-a <action_type>


ACTIONS

list-filters
    shows a list of the available filters


run-filter


-- load-csv <path>
-- run-filter <name>
-- list-filters


-h  help

        """


class commands:
    def quit(self):
        sys.exit(1)
    def set_mode(self):
        mode = 1
    def load_csv(self):
        csv_load.run(cnx, path)



command_dict = commands.__dict__


def main(argv):

    if(len(argv) != 1):
        print "Please provide database configuration file"
        sys.exit(1)

    print "Establishing connection to database"

    config =  argv[0]
    cnx = conn.get_connection(config)
    
    if cnx is None:
        sys.exit(1)
    else:
        print "Connected..."
    
    while True:
         
        command = raw_input('redwood$ ')
        func = None

        args = 
        try:
            func =  command_dict[command]
        except Exception, e:
            print "error"

        
        if(func is None):
            usage()
        else:
            func(command)
        


    cnx.close()

if __name__ == "__main__":
    main(sys.argv[1:])
