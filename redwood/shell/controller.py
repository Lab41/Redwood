import sys
import os
import getopt
import string
import exceptions
import multiprocessing
from modes import StandardMode
from redwood.filters import filter_list
import redwood.helpers.core as core

class SessionController:

    def __init__(self, cnx):
        self.cnx = cnx
        self.curr_filter_index = None
        self.mode_stack = [StandardMode(cnx, self)]
        
    def pushMode(self, new_mode):
        self.mode_stack.append(new_mode)
    def popMode(self):
        if(len(self.mode_stack) > 1):
            self.mode_stack.pop()        
    def run(self):
        
        print "...running with {} cores".format(multiprocessing.cpu_count())
        
        print "...loading filters from ./Filters directory if exists"
        core.import_filters("./Filters", self.cnx)

        while True:
            mode = self.mode_stack[len(self.mode_stack) - 1] 
            command = raw_input(mode.prompt)
            command = string.strip(command)
            args = string.split(command)
           
            if(len(args) == 0):
                mode.do_help()
                continue
           

            mode.execute(*args)
            


