from redwood.filters.redwood_filter import RedwoodFilter
import sys
import pkgutil
import inspect
import imp


filter_list = list()

for importer, modname, ispkg in pkgutil.iter_modules(__path__):
    module = __import__(modname,locals(),[],-1) 
    for name,cls in inspect.getmembers(module): 
        if inspect.isclass(cls) and issubclass(cls, RedwoodFilter):
            instance = cls()
            if(instance.name != "generic"):
                filter_list.append(instance)

