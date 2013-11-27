#!/usr/bin/env python
#
# Copyright (c) 2013 In-Q-Tel, Inc/Lab41, All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Created on 19 October 2013
@author: Lab41

This package provides connection functionality to a redwood MySQL db
"""


import sys
import os
import getopt
import string
import MySQLdb
import exceptions
import ConfigParser


def connect_with_config(config_path):
    """
    Given a path, returns a connection object

    :param config_path: path to the configuration file
    :return MySQL connection object
    """
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
