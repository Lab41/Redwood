import sys
import os
import getopt
import string
import exceptions
import multiprocessing
from redwood.shell.modes import StandardMode, FilterMode, DiscoverMode



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
        
        print "running with {} cores".format(multiprocessing.cpu_count())

        while True:
            mode = self.mode_stack[len(self.mode_stack) - 1] 
            command = raw_input(mode.prompt)
            command = string.strip(command)
            args = string.split(command)
           
            if(len(args) == 0):
                getattr(mode, 'help')()
                continue
           
            action = args[0]
            try:
                func = getattr(mode, action) 
            except AttributeError:
                print "Command \'{}\' not recognized".format(action)
                continue
            if func is not None:
                func(args[1:])
            


