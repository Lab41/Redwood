import cmd
import sys
import redwood.filters
from redwood.filters import filter_list

class SubInterpreterDiscover(cmd.Cmd):
    #prompt = "discover"

    def do_test(self, line):
        print "hi"

    def do_test_help(self, line):
        print "help"

    def preloop(self, line=None):
        if line:
            self.prompt = "discover"+line
            for plugin in filter_list:
                publicMethods = filter(lambda funcname: funcname.startswith('discover_'), dir(plugin)) 
                for method in publicMethods:
                    exec('SubInterpreterDiscover.'+method.replace("discover_", "do_", 1)+' = self.do_test')
                    exec('SubInterpreterDiscover.'+method.replace("discover_", "help_", 1)+' = self.do_test_help')

class SubInterpreterFilter(cmd.Cmd):
    prompt = "(level2) "

    def do_discover(self, line):
        if line:
            sub_cmd = SubInterpreterDiscover()
            sub_cmd.preloop(line)
            sub_cmd.cmdloop()
        else:
            print "Error: Filter Id required"


    def do_subcommand_2(self, line):
        pass

    def do_back(self, line):
        sys.stdout.write('\n')
        return True
    do_EOF = do_back

