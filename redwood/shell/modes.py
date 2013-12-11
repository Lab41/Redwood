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
from redwood.foundation.report import Report
import time
import traceback



class GeneralMode(object):

    def __init__(self, cnx, controller):
        self.cnx = cnx
        self.prompt = ""
        self.controller = controller
 
    def do_quit(self, args=None):
        '''quit: Exit the redwood console'''
        if self.cnx != None:
            self.cnx.close()
        sys.exit(1)
 
    def do_back(self, args=None):
        '''back: return up one level'''
        self.controller.popMode()
 
    def do_help(self, cmd=''):
        "Get help on a command. Usage: help command"
        if cmd: 
            func = getattr(self, 'do_' + cmd, None)
            if func:
                print func.__doc__
                return

        publicMethods = filter(lambda funcname: funcname.startswith('do_'), dir(self)) 
        commands = [cmd.replace('do_', '', 1) for cmd in publicMethods] 
        print ("Commands: " + " ".join(commands))

    def execute(self, cmd, *args):
        func = getattr(self, 'do_' + cmd, None)
        if not func:
            print "Command: %s is not valid" % cmd
            self.do_help()
            return
    
        try: 
            func(*args)
        except TypeError, e:
            traceback.print_exc()
            print "Error: %s" % e
    
    
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
 
    def do_help(self, args = None):
        if args and len(args) > 0:
            if self.plugin.do_help(args):
                return
        
        super(DiscoverMode, self).do_help(args)
        self.plugin.do_help()

    def execute(self, command, *args):
        try:
            if self.plugin.run_func(command, *args):
                return
                
            super(DiscoverMode, self).execute(command, *args)
        except TypeError, e:
            traceback.print_exc()
            print "Error: %s" % e

#
## FILTER MODE
#
class FilterMode(GeneralMode):
    
    def __init__(self, cnx, controller):
        super(FilterMode, self).__init__(cnx, controller)
        self.prompt = '\033[1;32mredwood-filter$ \033[1;m'   
        
    def do_discover(self, args = None):
        '''[*] discover <filter-id>\n\t|- activates discover mode for the given filter-id\n\t|-[filter-id]  - id of filter'''
        if not args or len(args) != 1:
            print "Error: Filter Id required"
            return
        v = GeneralMode.validateFilterId(args[0])       
        if v < 0:
            return
        self.controller.pushMode(DiscoverMode(self.cnx, filter_list[v],  self.controller))     

    def do_show_results(self, args = None):
        '''[*] show_results <filter-id> <direction> <count> <source> <out>\n\t|- shows the results for the given filters score table\n\t|-[filter-id]  - id of filter\n\t|-[direction]  - top or bottom\n\t|-[count]      - items to display\n\t|-[source]     - source name\n\t|-[out]        - file to write output to'''
        if len(args) != 5:
            print "Error: incorrect number of arguments"
            return
        v = GeneralMode.validateFilterId(args[0])
        plugin = filter_list[v]
        plugin.show_results(args[1], args[2], args[3], args[4])

    def do_update(self, args = None):
        '''update <filter-id> <source>'''
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
        
    def do_rebuild(self, args = None):
        '''[*] rebuild <filter-id>\n\t|-rebuilds all tables for the specified filter\n\t|-[filter-id]   - id of filter'''
        if(len(args) != 1):
            print "Error: Filter Id required"
            return
        v = GeneralMode.validateFilterId(args[0])
        if v<0:
            return
        plugin = filter_list[v]
        plugin.rebuild()
        print "completing analysis of data using filter \"{}\"".format(plugin.name)

    def do_clean(self, args=None):
        '''clean <filter-id'''
        v = GeneralMode.validateFilterId(args[0])
        if v < 0:
            return
        plugin = filter_list[v]
        plugin.clean()
        print "all data deleted associated with filter \"{}\"".format(plugin.name)

    def do_list(self, args = None):
        '''list: lists the avialble filters'''
        print "Available Filters"
        i = 0
        for plugin in filter_list:
            print "{}............{}".format(i, plugin.name)
            i+=1

    def do_aggregate_scores(self, args = None):
        '''[*] aggregate_scores (optional)<filter:weight>\n\t|- aggregates the reputations of all files using the list of filters and weights provided\n\t|- if no list is provided all filters are weighted equally\n\t|-[filter:weight]  - optional list of filter IDs and weights\n\t|- weights are a percentage and can range from 0-1 or 0-100'''
        print "Aggregating Scores"
        ag = Aggregator(self.cnx)
        if args and len(args) > 0:
            ag.aggregate(filter_list, args)
        else:
            ag.aggregate(filter_list)

    def do_run_report_survey(self, source = None):
        '''[*] run_survey (optional)<source_name>\n\t|- runs the survey function for the given source\n\t |- if no source is provided run_survey processes all sources\n\t|-[source_name] - option name of source to process'''
        rpt = Report(self.cnx)
        if source == None:
            sources = core.get_all_sources(self.cnx)
            for s in sources:
                print "Running Report Survey for: " + s.source_name
                rpt.run_filter_survey(s.source_name)
                rpt.generate_report(s)
        else:
            src = core.get_source_info(self.cnx, source)
            if src == None:
                print "Source " + source + " does not exist"
                return
            print "Running Report Survey for: " + src.source_name
            rpt.run_filter_survey(src.source_name)
            rpt.generate_report(src)

class StandardMode(GeneralMode):
    def __init__(self, cnx, controller):
        super(StandardMode, self).__init__(cnx, controller)
        self.prompt = '\033[1;32mredwood$ \033[1;m'

    def do_filter(self):
        '''[*] filter\n\t|--activates FILTER mode:'''

        self.controller.pushMode(FilterMode(self.cnx, self.controller))

    def do_load_csv(self, path, survey):
        '''[*] load_csv <path> <include-survey>
            |-[path]   - path where csv files exist or a path to a csv file
            |-[survey] - either set as \"yes\" or \"no\" if you want to include the survey''' 
        if survey in ( "yes", "Yes", "YES", "1" ) :
            choice = True
        elif survey in ( "no", "No", "NO", "0" ) :
            choice = False
        else:
            print "Error: Please specify \"yes\" or \"no\" if you want a survey"
            return

        csv_load.run(self.cnx, path, choice)

    def do_import_filters(self, path):
        '''[*] import_filters <path>\n\t|-[path]   - path to the directory containing the filters'''
        new_filters = core.import_filters(path, self.cnx)
        print "New Filters: "
        print new_filters
        
