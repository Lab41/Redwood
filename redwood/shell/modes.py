import sys
import os
import getopt
import string
import exceptions
import redwood.filters
import redwood.io.csv_importer as csv_load
from redwood.filters import filter_list
import redwood.helpers.core as core
from redwood.foundation.aggregator import Aggregator
import time



class GeneralMode(object):

    def __init__(self, cnx, controller):
        self.cnx = cnx
        self.prompt = ""
        self.controller = controller
    def quit(self, args=None):
        if self.cnx != None:
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
        
        if(value < 0 or value >= len(filter_list)):
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
    def run(self, args = None):
        
        if(len(args) > 0):
            try:
                start_time = time.time()
                method = "discover_{}".format(args[0])
                self.plugin.run_func(method, args[1:])
                print "...elapsed time was {}".format(time.time() - start_time)
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
        self.prompt = '\033[1;32mredwood-filter$ \033[1;m'   
    def discover(self, args = None):
        if(len(args) != 1):
            print "Error: Filter Id required"
            return
        v = GeneralMode.validateFilterId(args[0])       
        if v < 0:
            return
        self.controller.pushMode(DiscoverMode(self.cnx, filter_list[v],  self.controller))     
    def show_results(self, args = None):
        if len(args) != 5:
            print "Error: incorrect number of arguments"
            return
        v = GeneralMode.validateFilterId(args[0])
        plugin = filter_list[v]
        plugin.show_results(args[1], args[2], args[3], args[4])

    def update(self, args = None):
        if len(args) != 3:
            print "Error: incorrect number of arguments"
            return
        v = GeneralMode.validateFilterId(args[0])
        if v < 0:
            return

        cursor = self.cnx.cursor()
        query = "select * from media_source where name=\"{}\"".format(args[1])
        cursor.execute(query)
        r = cursor.fetchone()
        if r is not None and args[2] != "Force":
            print "Filters should have already been applied. Use the \"Force\" Luke, if you still want to run Update"
            return
        plugin = filter_list[v]
        start_time = time.time()
        plugin.update(args[1])
        elapsed_time = time.time() - start_time
        print "completed update of media source \"{}\" for filter \"{}\" in {} seconds".format(args[1], plugin.name, elapsed_time)
        
    def rebuild(self, args = None):
        if(len(args) != 1):
            print "Error: Filter Id required"
            return
        v = GeneralMode.validateFilterId(args[0])
        plugin = filter_list[v]
        plugin.rebuild()
        print "completing analysis of data using filter \"{}\"".format(plugin.name)
    def clean(self, args=None):
        v = GeneralMode.validateFilterId(args[0])
        if v < 0:
            return
        plugin = filter_list[v]
        plugin.clean()
        print "all data deleted associated with filter \"{}\"".format(plugin.name)
    def list(self, args=None):
        print "Available Filters"
        i = 0
        for plugin in filter_list:
            print "{}............{}".format(i, plugin.name)
            i+=1
    def aggregate_scores(self, args = None):
        print "Aggregating Scores"
        ag = Aggregator(self.cnx)
        if len(args) > 0:
            ag.aggregate(filter_list, args)
        else:
            ag.aggregate(filter_list)
    def help(self, args=None):
        print "Filter Mode"
        print "[*] list"
        print "\t|- lists all loaded filters"
        print "[*] discover <filter-id>"
        print "\t|- activates discover mode for the given filter-id"
        print "\t|-[filter-id]  - id of filter"
        print "[*] rebuild <filter-id>"
        print "\t|-rebuilds all tables for the specified filter"
        print "\t|-[filter-id]   - id of filter"
        print "[*] show_results <filter-id> <direction> <count> <source> <out>"
        print "\t|- shows the results for the given filters score table"
        print "\t|-[filter-id]  - id of filter"
        print "\t|-[direction]  - top or bottom"
        print "\t|-[count]      - items to display"
        print "\t|-[source]     - source name"
        print "\t|-[out]        - file to write output to"
        print "[*] aggregate_scores (optional)<filter:weight>"
        print "\t|- aggregates the reputations of all files using the list of filters and weights provided"
        print "\t|- if no list is provided all filters are weighted equally"
        print "\t|-[filter:weight]  - optional list of filter IDs and weights"
        print "\t|- weights are a percentage and can range from 0-1 or 0-100"
        
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
    def import_filters(self, args=None):
        if len(args) != 1:
            print "Error: pat required"
            return
        new_filters = core.import_filters(args[0])
        print "New Filters: "
        print new_filters
    def help(self, args = None):
        print         '\n[*] filter \n' \
        '\t-- activates FILTER mode \n' \
        '[*] load_csv <path> \n' \
        '\t-- loads a redwood csv file from the given path\n' \
        '[*] import_filters <path>\n' \
        '\t--loads all filter modules from the specified path'



