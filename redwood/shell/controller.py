import cmd
import sys
import redwood.helpers.core as core
import redwood.io.csv_importer as csv_load
from modes import SubInterpreterFilter
from redwood.filters import filter_list

class SessionController(cmd.Cmd):
    prompt = '\033[1;32mredwood$ \033[1;m'

    def default(self, line):
        if line == 'EOF' or line == 'exit':
            self.do_quit(line)
            return True
        else:
            print "*** Command not recognized, try 'help'"

    def emptyline(self):
        pass

    def preloop(self, cnx=None):
        self.cnx = cnx

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
        sub_cmd = SubInterpreterFilter()
        sub_cmd.preloop(self.cnx)
        sub_cmd.cmdloop()

    def do_import_filters(self, line):
        '''[*] import_filters <path>\n\t|-[path]   - path to the directory containing the filters'''
        print self.cnx
        new_filters = core.import_filters(line, self.cnx)
        print "New Filters: "
        print new_filters

    def do_load_csv(self, line):
        '''[*] load_csv <path> <include-survey>
            |-[path]   - path where csv files exist or a path to a csv file
            |-[survey] - either set as \"yes\" or \"no\" if you want to include the survey'''
        try:
            line = line.strip().split()
            path = line[0]
            survey = line[1]
            choice = False

            if survey in ( "y", "Y", "yes", "Yes", "YES", "1" ) :
                choice = True
            elif survey in ( "n", "N", "no", "No", "NO", "0" ) :
                choice = False
            else:
                print "Error: Please specify \"yes\" or \"no\" if you want a survey"
                return

            csv_load.run(self.cnx, path, choice)
        except:
            print "Error: Please specify a path and whether or not to include the survey"
        return

    def do_quit(self, line):
        '''quit: Exit the redwood console'''
        if self.cnx != None:
            self.cnx.close()
        sys.stdout.write('\n')
        return True
