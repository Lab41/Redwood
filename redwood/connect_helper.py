import mysql.connector 
import ConfigParser
from mysql.connector.constants import ClientFlag

def get_connection(config_file):

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
    
    #so we can send raw files to the mySQL server for the LOAD command
    extra_flags = [ClientFlag.LOCAL_FILES]

    try:

        cnx = mysql.connector.connect(user=user, password=password, host=host, database=database, client_flags=extra_flags)
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
   
    return cnx

