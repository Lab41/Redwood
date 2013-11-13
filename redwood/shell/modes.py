import sys
import os
import getopt
import string
import exceptions
import redwood.filters
import redwood.io.csv_importer as csv_load
from redwood.filters import plugins
import time



class GeneralMode(object):

    def __init__(self, cnx, controller):
        self.cnx = cnx
        self.prompt = ""
        self.controller = controller
    def quit(self, args=None):
        self.cnx.close()
        sys.exit(1)
    def back(self, args=None):
        self.controller.popMode()
    @staticmethod
    def validateFilterId(str_val):

        try:
            value = int(str_val)
        except exceptions.ValueError:
            print "Error: \'{}\' is not a number".format(str_val)
            return -1
        
        if(value < 0 or value >= len(plugins)):
            print "Error: no plugin exists for that number"     
            return -1

        return value


#
##DISCOVER MODE####
#
class DiscoverMode(GeneralMode):
  
    def __init__(self, cnx, plugin, controller):
        super(DiscoverMode, self).__init__(cnx, controller)
        self.plugin = plugin
        self.prompt = '\033[1;32mredwood-{}-discover$ \033[1;m'.format(plugin.name)
        self.plugin.cnx = cnx
    def run(self, args = None):
        
        if(len(args) > 0):
            try:
                method = "discover_{}".format(args[0])
                f = self.plugin.__getattribute__(method)
                f(*args[1:])
            except TypeError as e:
                self.plugin.usage()
                print e
                return
            except Exception, e:
                print e
                return 
    
    def help(self, args = None):
        print "Discover Mode"
        print "==================="
        print "[*] quit, back, help"
        print "[*] run <Discover-command> <Discover-args>"
        print "     -- runs a discover command for the given filter"
        print "-------------------"
        self.plugin.usage()


#
## FILTER MODE
#
class FilterMode(GeneralMode):
    
    def __init__(self, cnx, controller):
        super(FilterMode, self).__init__(cnx, controller)
        self.prompt = '\033[1;32mredwood-filters$ \033[1;m'   
    def discover(self, args = None):
        if(len(args) != 1):
            print "Error: Filter Id required"
            return
        v = GeneralMode.validateFilterId(args[0])       
        if v < 0:
            return
        self.controller.pushMode(DiscoverMode(self.cnx, plugins[v],  self.controller))     

    def update(self, args = None):
        if len(args) != 2:
            print "Error: source required"
            return
        v = GeneralMode.validateFilterId(args[0])
        if v < 0:
            return

        cursor = self.cnx.cursor()
        query = "select * from media_source where name=\"{}\"".format(args[1])
        cursor.execute(query)
        r = cursor.fetchone()

        plugin = plugins[v]
        plugin.cnx = self.cnx
        start_time = time.time()
        plugin.update(args[1])
        elapsed_time = time.time() - start_time
        print "completed update of media source \"{}\" for filter \"{}\" in {} seconds".format(args[1], plugin.name, elapsed_time)
        
    def clean_run(self, args = None):
        if(len(args) != 1):
            print "Error: Filter Id required"
            return
        v = GeneralMode.validateFilterId(args[0])
        plugin = plugins[v]
        plugin.cnx = self.cnx
        plugin.run()
        print "completing analysis of data using filter \"{}\"".format(plugin.name)
    def clean(self, args=None):
        v = GeneralMode.validateFilterId(args[0])
        if v < 0:
            return
        plugin = plugins[v]
        plugin.cnx = self.cnx
        plugin.clean()
        print "all data deleted associated with filter \"{}\"".format(plugin.name)
    def list(self, args=None):
        print "Available Filters"
        i = 0
        for plugin in plugins:
            print "{}............{}".format(i, plugin.name)
            i+=1
    def help(self, args=None):
        print( "Filter Mode")
        print("[*] list") 
        print("\t-- lists all loaded filters ")
        print("[*] discover <filter-id> ")
        print("\t-- activates discover mode for the given filter-id")
        print "[*] clean_run <filter-id>"
        print "\t--clears all data for a filter, then runs the filter on all sources"
        print "[*] update <filter-id> <source-name>"
        print "\t-- updates the data model to include data from source <source-name>"
        print "[*] clean <filter-id>"
        print "\t-- removes all data associated with the filter"
class StandardMode(GeneralMode):
    def __init__(self, cnx, controller):
        super(StandardMode, self).__init__(cnx, controller)
        self.prompt = '\033[1;32mredwood$ \033[1;m'
    def filter(self, args=None):
        self.controller.pushMode(FilterMode(self.cnx, self.controller))
    def load_csv(self, args=None):
        if(len(args) != 1):
            print "Error: path required"
            return
        csv_load.run(self.cnx, args[0])
    def help(self, args = None):
        print '\n[*] filter \n' \
        '\t-- activates FILTER mode \n' \
        '[*] load_csv <path> \n' \
        '\t-- loads a redwood csv file from the given path'



