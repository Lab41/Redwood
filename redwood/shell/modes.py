import cmd
import exceptions
import sys
import time
import shlex
import redwood.filters
import redwood.helpers.core as core
from redwood.filters import filter_list
from redwood.foundation.aggregator import Aggregator
from redwood.foundation.report import Report

class SubInterpreterDiscover(cmd.Cmd):
    
    def __init__(self, cnx, line):
        cmd.Cmd.__init__(self)
        self.cnx = cnx

        if line:
            self.plugin = filter_list[int(line)]
            self.prompt = '\033[1;32mredwood-'+str(self.plugin.name)+'-discover$ \033[1;m'
            publicMethods = filter(lambda funcname: funcname.startswith('discover_'), dir(self.plugin)) 
            self.added_attrs = []
            for method in publicMethods:
                self.added_attrs.append(method.replace("discover_", "do_", 1))
                setattr(SubInterpreterDiscover, method.replace("discover_", "do_", 1), self.run)


    def default(self, line):
        if line == 'EOF' or line == 'exit' or line == 'quit':
            self.do_back(line)
            return True
        else:
            print "*** Command not recognized, try 'help'"

    def emptyline(self):
        pass

    def help_help(self):
        self.do_help('')

    def do_back(self, line):
        '''Go back a level in the shell'''
        for attr in self.added_attrs:
            delattr(SubInterpreterDiscover, attr)
        return True

    def run(self, line):
        '''Calls out the run_func in redwood_filter'''
        if line:
            #line_a = self.cmdline.split()
            line_a = shlex.split(self.cmdline)
            func_name = line_a[0]
            args = tuple(line_a[1:])
            self.plugin.run_func(func_name, *args)
        else:
            self.plugin.do_help(self.cmdline)
        
    def do_help(self, line):
        if line:
            self.plugin.do_help(line)
        else:
            cmd.Cmd.do_help(self, line)

    def precmd(self, line):
        self.cmdline = line
        return line

    def do_quit(self, line):
        '''quit: Exit the redwood console'''
        if self.cnx != None:
            self.cnx.close()
        sys.stdout.write('\n')
        sys.exit(0)

class SubInterpreterFilter(cmd.Cmd):
    prompt = '\033[1;32mredwood-filter$ \033[1;m'
        
    def __init__(self, cnx):
        cmd.Cmd.__init__(self)
        self.cnx = cnx
    
    def do_quit(self, line):
        '''quit: Exit the redwood console'''
        if self.cnx != None:
            self.cnx.close()
        sys.stdout.write('\n')
        sys.exit(0)

    def default(self, line):
        if line == 'EOF' or line == 'exit' or line == 'quit':
            self.do_back(line)
            return True
        else:
            print "*** Command not recognized, try 'help'"

    def emptyline(self):
        pass

    def help_help(self):
        self.do_help('')

    def do_back(self, line):
        '''Go back a level in the shell'''
        return True

    def do_discover(self, line):
        '''
        discover <filter-id>
        
        activates discover mode for the given filter with id "filter-id"
        '''
        if line:
            v = SubInterpreterFilter.validateFilterId(line)
            if v >= 0:
                sub_cmd = SubInterpreterDiscover(self.cnx, line)
                sub_cmd.cmdloop()
        else:
            print "Error: Filter Id required"

    def do_show_results(self, line):
        '''
        show_results <filter-id> <direction> <count> <source> <out>
        
        shows the results for the given filter's score table
        
        filter-id   - id of filter
        direction   - top or bottom
        count       - items to display
        source      - source name
        out         - file to write output to (optional)
        '''
        args = line.split()
        if len(args) != 5 and len(args) != 4 :
            print "Error: incorrect number of arguments"
            return
        v = self.validateFilterId(args[0])
        plugin = filter_list[v]
        plugin.show_results(*args[1:])


    def do_rerun(self, line):
        '''
        rerun <filter-id>
        
        Reruns a filter on all sources
        '''
        args = line.split()
        if(len(args) != 1):
            print "Error: Filter Id required"
            return
        
        v = self.validateFilterId(args[0])
        if v < 0:
            return
        plugin = filter_list[v]
        plugin.clean()
        
        print "Deleting old data in filter storage"

        sources = core.get_all_sources(self.cnx)
       
        print "Creating new data"
        for src_info in sources:
            print "Running filter on source: {}".format(src_info.source_name)
            plugin.update(src_info.source_name)

        print "Rerun complete"

    def do_list(self, line):
        '''list: lists the avialble filters'''
        print "Available Filters"
        i = 0
        for plugin in filter_list:
            print "{}............{}".format(i, plugin.name)
            i+=1

    def do_aggregate_scores(self, line):
        '''
        aggregate_scores  filter_id:weight filter_id:weight ...
        
        Aggregates the reputations of all files using the list of filters and weights provided. If no list is
        provided, all filters are weighted equally. The "filter_id" is the numeric id of the filter.  The "weight"
        is a percentage between 0-100, such that the total of all specified weights is 100. 

        For example, if you have 3 filters loaded, and you want to aggregate the scores such that the distribution of weights
        is 50, 30, 20 respectively, then you would run the following command

        Example

        aggregate_scores 0:50 1:30 2:20
        '''
        
        print "Aggregating Scores"
        args = line.split()
        ag = Aggregator(self.cnx)
        if args and len(args) > 0:
            ag.aggregate(filter_list, args)
        else:
            ag.aggregate(filter_list)

    def do_run_survey(self, line):
        '''[*] run_survey <source_name>\n\t|- runs the survey function for the given source\n\t |- if no source is provided run_survey processes all sources\n\t|-[source_name] - option name of source to process'''

	args = shlex.split(line)
	
	if len(args) < 1:
            print "Error: Incorrect # of arguments"
	    return

        src_obj = core.get_source_info(self.cnx, args[0])
        
        if src_obj is None:
            print "Error: Unable to find source {}".format(args[0])
            return
        else:
            rpt = Report(self.cnx, src_obj)
	    if len(args) > 1:
        	rpt.run(args[1:])
	    else:
		rpt.run(None)

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
