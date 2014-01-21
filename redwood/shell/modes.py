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
            print "line a: {}".format(line_a)
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
        '''[*] discover <filter-id>\n\t|- activates discover mode for the given filter-id\n\t|-[filter-id]  - id of filter'''
        if line:
            v = SubInterpreterFilter.validateFilterId(line)
            if v >= 0:
                sub_cmd = SubInterpreterDiscover(self.cnx, line)
                sub_cmd.cmdloop()
        else:
            print "Error: Filter Id required"

    def do_show_results(self, line):
        '''[*] show_results <filter-id> <direction> <count> <source> <out>\n\t|- shows the results for the given filters score table\n\t|-[filter-id]  - id of filter\n\t|-[direction]  - top or bottom\n\t|-[count]      - items to display\n\t|-[source]     - source name\n\t|-[out]        - file to write output to'''
        args = line.split()
        if len(args) != 5:
            print "Error: incorrect number of arguments"
            return
        v = self.validateFilterId(args[0])
        plugin = filter_list[v]
        plugin.show_results(args[1], args[2], args[3], args[4])

    def do_update(self, line):
        '''update <filter-id> <source> <force?>'''
        args = shlex.split(line)
        if len(args) != 3:
            print "Error: incorrect number of arguments"
            return
        v = self.validateFilterId(args[0])
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

    def do_rebuild(self, line):
        '''[*] rebuild <filter-id>\n\t|-rebuilds all tables for the specified filter\n\t|-[filter-id]   - id of filter'''
        args = line.split()
        if(len(args) != 1):
            print "Error: Filter Id required"
            return
        v = self.validateFilterId(args[0])
        if v<0:
            return
        plugin = filter_list[v]
        plugin.rebuild()
        print "completing analysis of data using filter \"{}\"".format(plugin.name)

    def do_clean(self, line):
        '''clean <filter-id>'''
        args = line.split()
        v = self.validateFilterId(args[0])
        if v < 0:
            return
        plugin = filter_list[v]
        plugin.clean()
        print "all data deleted associated with filter \"{}\"".format(plugin.name)

    def do_list(self, line):
        '''list: lists the avialble filters'''
        print "Available Filters"
        i = 0
        for plugin in filter_list:
            print "{}............{}".format(i, plugin.name)
            i+=1

    def do_aggregate_scores(self, line):
        '''[*] aggregate_scores (optional)<filter:weight>\n\t|- aggregates the reputations of all files using the list of filters and weights provided\n\t|- if no list is provided all filters are weighted equally\n\t|-[filter:weight]  - optional list of filter IDs and weights\n\t|- weights are a percentage and can range from 0-1 or 0-100'''
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
