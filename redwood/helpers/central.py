import sys
import os
import inspect
from redwood.filters import RedwoodFilter
from redwood.filters import filter_list


def import_filters(path):
    """
    Imports filters from an external directory at runtime. Imported filters will be added
    to the global filter_list

    :param path: path where the modules reside
    :return list of newly add filter instances
    """

    new_filters = list()

    #make sure path exists
    if os.path.isdir(path) is False:
        print "Error: path {} does not exist".format(path)
        return None

    #add the path to the PYTHONPATH
    sys.path.append(path)

    #acquire list of files in the path
    mod_list = os.listdir(path)

    for f in mod_list:

        print f
        #continue if it is not a python file
        if f[-3:] != '.py':
            continue    

        #get module name by removing extension
        mod_name = os.path.basename(f)[:-3]

        #import the module
        module = __import__(mod_name, locals(), globals())
        for name,cls in inspect.getmembers(module): 
            #check name comaprison too since RedwoodFilter is a subclass of itself
            if inspect.isclass(cls) and issubclass(cls, RedwoodFilter) and name != "RedwoodFilter":
                instance = cls()
                #append an instance of the class to the filter_list
                filter_list.append(instance)
                new_filters.append(instance)

    return new_filters


