#!/usr/bin/env python

import sys
import os
import getopt
import string
import MySQLdb
import exceptions
import ConfigParser



def connect_from_file(config_file):

    cnx = None
    
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
    
    return cnx

def connect_from_env():
    pass
