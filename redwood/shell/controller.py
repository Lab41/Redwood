import cmd
import sys
import shlex
import redwood.helpers.core as core
import redwood.io.csv_importer as csv_load
from modes import SubInterpreterFilter
from redwood.filters import filter_list

class SessionController(cmd.Cmd):
    prompt = '\033[1;32mredwood$ \033[1;m'


    def __init__(self, cnx):
        cmd.Cmd.__init__(self) 
        self.cnx = cnx

    def default(self, line):
        if line == 'EOF' or line == 'exit':
            self.do_quit(line)
            return True
        else:
            print "*** Command not recognized, try 'help'"

    def emptyline(self):
        pass

    def preloop(self, cnx=None):
        #self.cnx = cnx
        pass
    def cmdloop(self):
        try:
            if not filter_list:
                core.import_filters("./Filters", self.cnx)
            return cmd.Cmd.cmdloop(self)
        except KeyboardInterrupt:
            sys.stdout.write('\n')
            return self.cmdloop()

    def help_help(self):
        self.do_help('')

    def do_filter(self, line):
        '''[*] filter\n\t|--activates FILTER mode:'''
        sub_cmd = SubInterpreterFilter(self.cnx)
        sub_cmd.cmdloop()

    def do_import_filters(self, line):
        '''[*] import_filters <path>\n\t|-[path]   - path to the directory containing the filters'''
        new_filters = core.import_filters(line, self.cnx)
        
        if new_filters is not None:
            print "Importing the following filters: "
            for f in new_filters:
                print "{}".format(f.name)
        else:
            print "No filters found"
            
    def do_load_csv(self, line):
        '''[*] load_csv <path> <include-survey>
            |-[path]   - path where csv files exist or a path to a csv file
         '''
        try:
            csv_load.run(self.cnx, line)
        except Exception as e:
            print "Error occurred {}".format(e)
        return

    def do_quit(self, line):
        '''quit: Exit the redwood console'''
        if self.cnx != None:
            self.cnx.close()
        sys.stdout.write('\n')
        print "quitting"
        sys.exit(0)
        return True
