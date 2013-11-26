#!/usr/bin/env python

import sys
import os
import getopt
import string
import MySQLdb
import exceptions
import ConfigParser


def connect_with_config(config_path):

    cnx = None
    
    if config_path is None:
        print "Error: A config file must be provided"
        return cnx
     
    try:
        with open(config_path): pass
    except IOError:
        print ('Error: Configuration file \'{}\' not found'.format(config_path))
        return cnx

    config = ConfigParser.RawConfigParser()
    config.read(config_path)
    user        =       config.get("mysqld", "username")
    password    =       config.get("mysqld", "password")
    host        =       config.get("mysqld", "host")
    database    =       config.get("mysqld", "database")
    
    try:

        cnx = MySQLdb.connect(host=host, user=user, passwd=password, db=database, local_infile=1)
    except MySQLdb.Error as e:
        print(e)
        return None

    if cnx is None:
        print "Error: Unable to connect to database"
        return None
    
    return cnx 
