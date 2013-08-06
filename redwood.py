import sys
import os
import getopt
import string
import exceptions
import redwood.filters
import redwood.connect_helper as conn
import redwood.io.csv_importer as csv_load
from redwood.filters import plugins


def usage():
    m = "STANDARD"
    global curr_mode
    if(curr_mode == MODE_FILTER):
        m = "FILTER"
    elif(curr_mode == MODE_DISCOVER):
        m = "DISCOVER"

    print "USAGE FOR {} MODE".format(m)
    print "========================="
    print '''[*] quit, back, help
    -standard commands'''

curr_filter_index = -1
MODE_BASE = 0
MODE_FILTER = 1
MODE_DISCOVER = 2
curr_mode = MODE_BASE


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
class mode_discover:
    def run(cnx, args = None):
        plugin = plugins[curr_filter_index]
        plugin.cnx = cnx
        if(len(args) > 0):
            try:
                method = "discover_{}".format(args[0])
                dir(plugin)
                f = plugin.__getattribute__(method)
                f(*args[1:])
            except TypeError as e:
                plugin.usage()
                print e
                return
            except Exception, e:
                print e
                return 
    def quit(cnx, args = None):
        cnx.close()
        sys.exit()
    def back(cnx, args = None):
        global curr_mode
        curr_mode = MODE_FILTER
    def help(cnx=None, args = None):
        plugin = plugins[curr_filter_index]
        usage()
        print "[*] run <Discover-command> <Discover-args>"
        print "     -- runs a discover command for the given filter"
        print "-------------------"
        plugin.usage()

class mode_filter:
    def quit(cnx, args):
        cnx.close()
        sys.exit()
    def discover(cnx, args = None):
        if(len(args) != 1):
            print "Error: Filter Id required"
            return
        v = validateFilterId(args[0])       
        if v < 0:
            return

        global curr_filter_index
        global curr_mode
        curr_mode = MODE_DISCOVER
        curr_filter_index = v
    def display(cnx, args=None):
        global curr_filter_index
        plugin = plugins[curr_filter_index]
        plugin.display()
    def run(cnx, args = None):
        global curr_filter_index
        plugin = plugins[curr_filter_index]
        plugin.run(cnx)
    def back(cnx, args = None):
        global curr_mode
        curr_mode = MODE_BASE
    def list(cnx, args=None):
        print "Available Filters"
        i = 0
        for plugin in plugins:
            print "{}............{}".format(i, plugin.name)
            i+=1
    def help(cnx=None, args=None):
        usage()
        print '''[*] list 
    -- lists all loaded filters
[*] discover <filter-id>
    -- activates discover mode for the given filter-id
[*] run <filter-id>
    -- runs the filter with the given filter-id'''

class mode_base():
    def quit(cnx, args=None):
        cnx.close()
        sys.exit(1)
    def filter(cnx, args=None):
        global curr_mode
        curr_mode = MODE_FILTER
    def load_csv(cnx, args=None):
        csv_load.run(cnx, path)
    def help(cnx=None, args = None):
        usage()
        print '''[*] filter
    -- activates FILTER mode
[*] load_csv <path>
    -- loads a redwood csv file from the given path'''

mode_discover_dict = mode_discover.__dict__
mode_filter_dict = mode_filter.__dict__
mode_base_dict = mode_base.__dict__




#print '\033[1;30mGray like Ghost\033[1;m'
#print '\033[1;31mRed like Radish\033[1;m'
#print '\033[1;32mGreen like Grass\033[1;m'
#print '\033[1;33mYellow like Yolk\033[1;m'
#print '\033[1;34mBlue like Blood\033[1;m'
#print '\033[1;35mMagenta like Mimosa\033[1;m'
#print '\033[1;36mCyan like Caribbean\033[1;m'
#print '\033[1;37mWhite like Whipped Cream\033[1;m'
#print '\033[1;38mCrimson like Chianti\033[1;m'
#print '\033[1;41mHighlighted Red like Radish\033[1;m'
#print '\033[1;42mHighlighted Green like Grass\033[1;m'
#print '\033[1;43mHighlighted Brown like Bear\033[1;m'
#print '\033[1;44mHighlighted Blue like Blood\033[1;m'
#print '\033[1;45mHighlighted Magenta like Mimosa\033[1;m'
#print '\033[1;46mHighlighted Cyan like Caribbean\033[1;m'
#print '\033[1;47mHighlighted Gray like Ghost\033[1;m'
#print '\033[1;48mHighlighted Crimson like Chianti\033[1;m'


def main(argv):

    if(len(argv) != 1):
        print "Please provide database configuration file"
        sys.exit(1)

    print '\033[1;31m\n\n#################################\nWelcome to Redwood\n#################################\n\033[1;m'

    print "Establishing connection to database...",


    config =  argv[0]
    cnx = conn.get_connection(config)
   
    if cnx is None:
        sys.exit(1)
    else:
        print "connected"
   
    curr_command_set = mode_base_dict

    while True:
       

        if(curr_mode == MODE_BASE):
            prompt =  '\033[1;32mredwood$ \033[1;m' 
            curr_command_set = mode_base_dict
        elif(curr_mode == MODE_FILTER):
            global curr_filter_index
            prompt =  '\033[1;32mredwood-filters$ \033[1;m'
            curr_command_set = mode_filter_dict
        elif(curr_mode == MODE_DISCOVER):
            global curr_filter_index
            prompt =  '\033[1;32mredwood-{}-dicover$ \033[1;m'.format(plugins[curr_filter_index].name)
            curr_command_set = mode_discover_dict
            

        command = raw_input(prompt)
        command = string.strip(command)
    
        args = string.split(command)
        size = len(args)
        
        if(size == 0):
            curr_command_set['help']()
            continue
        
        func = None
 
        try:
            func = curr_command_set[args[0]]
        except Exception, e:
            curr_command_set['help']()
            print "command \'{}\' not recognized".format(args[0])
            
        if func is not None:
            func(cnx, args[1:])
        

    cnx.close()

if __name__ == "__main__":
    main(sys.argv[1:])
