import sys
import os
import getopt
import string
import MySQLdb
import exceptions
import ConfigParser
from redwood.shell.controller import SessionController



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

    print "Establishing connection to database...\n",

    cnx = None
    
    config_file = argv[0]
    
    try:
        with open(config_file): pass
    except IOError:
        print ('Configuration file \'{}\' not found. Please create the file '
                'in the local directory. Refer to README for required values'.format(config_file))
        return cnx



    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    user        =       config.get("mysqld", "username")
    password    =       config.get("mysqld", "password")
    host        =       config.get("mysqld", "host")
    database    =       config.get("mysqld", "database")
    
    cnx = None

    try:
        cnx = MySQLdb.connect(host=host, user=user, passwd=password, db=database, local_infile=1)
    except MySQLdb.Error as e:
        print(e)


    if(cnx == None):
        print "An error occurred while trying to establish a connection to the databse"
        sys.exit(1)
    
    
    SessionController(cnx).run()
    cnx.close()

if __name__ == "__main__":
    main(sys.argv[1:])
