import sys
import subprocess
import datetime
import hashlib
import time
import struct 
import binascii
import os
import re
from Queue import Queue

# 8 byte unique ID generator give a path.
#   - first five bytes are first five from sha1 of path name
#   - last 3 are the first three from the current time
# Returns a long
def generateUniqueId(path):
    
    m = hashlib.md5()
    m.update(path)
    first_five = m.digest()[:5]
    last_three = struct.pack("I", int(time.time()))[:3]
    combined = first_five + last_three
    return  long(binascii.hexlify(combined), 16) 


def write_stat_info(basename, dirname, file_id,  parent_id, file_handle):
   
    path = dirname + '/' + basename
    try:
        stat_obj = os.stat(path)
    except Exception:
        print "Error trying to stat {}".format(path)
        return

    p = subprocess.Popen(['file', '-b', '--mime-type', path], stdout=subprocess.PIPE)
    p.wait()
    file_type = p.stdout.read().rstrip()
    hash_val = ""
    if(os.path.isdir(path) != True and file_type != 'inode/chardevice' \
            and file_type != 'inode/symlink' \
            and file_type != 'inode/socket' \
            and file_type != 'inode/blockdevice' \
            and file_type != 'inode/x-empty' \
            and file_type != 'application/x-coredump'):
    
        hash_val = hash_file(path)
   
    data = {\
            'hash':hash_val, \
            'file_id':file_id, \
            'dirname':dirname, \
            'parent_id':parent_id, \
            'filename':basename, \
            'inode':stat_obj.st_ino, \
            'device':stat_obj.st_dev, \
            'permissions':str(oct(stat_obj.st_mode)), \
            'uid':stat_obj.st_uid, \
            'gid':stat_obj.st_gid, \
            'size':stat_obj.st_size, \
            'create_time':long(os.path.getctime(path)), \
            'access_time':long(stat_obj.st_atime), \
            'mod_time':long(stat_obj.st_mtime), \
            'metadata_change_time':long(stat_obj.st_ctime), \
            'user_flags':"", \
            'links':stat_obj.st_nlink, \
            'disk_offset':"", \
            'entropy':'', \
            'file_content_status':'', \
            'extension':os.path.splitext(basename)[1], \
            'file_type':file_type,
           }
  
    file_handle.write("{file_id:d},{parent_id:d},{dirname:s},{filename:s},{hash},"
            "{inode:d},{device:d},{permissions:s},{uid:d},{gid:g},{size:d},"
            "{create_time:d},{access_time:d},{mod_time:d},{metadata_change_time:d},"
            "{user_flags:s},{links:d},{disk_offset},{entropy},"
            "{file_content_status},{extension:s},{file_type:s}\n".format(**data))


BUFFER = 4096

def hash_file(fpath):
    
    h = hashlib.sha1()

    with open(fpath, 'r') as f:
        data = f.read(BUFFER)
        while(len(data)>0):
            h.update(data)
            data = f.read(BUFFER)
    f.close()
    return h.hexdigest()
    



def main(argv):

    if(len(argv) != 3):
        print "filewalk.py <directory> <unique-source-id>"
        return

    today = datetime.date.today()
    str_date = today.strftime('%d-%m-%Y')
    out_file = "{}-redwood.csv".format(str_date) 
    start_dir = argv[1]
    
    queue = Queue()


    with open(out_file, "w") as file_handle:
        
        file_handle.write("file_id, parent_id,dirname,basename,hash,fs_id,device,permissions,"
                "uid,gid,size,create_time,access_time,mod_time,metadata_change_time,"
                "file_content_status,extensions,file_type\n")

        #start the queue with a 0 value
        queue.put(0L) 
        
        for root, dirs, files in os.walk(start_dir):
            
            parent_id = queue.get()

            #optimization to skip proc fs 
            if(root.startswith('/proc')):
                continue

            new_parent_id = generateUniqueId(root)
            
            #write the parent directory
            write_stat_info("/", root,  new_parent_id, parent_id, file_handle)
            
            for d in dirs:
                queue.put(new_parent_id)

            for f in files:
                _id = generateUniqueId(f)
                write_stat_info(f, root, _id, new_parent_id, file_handle)

    file_handle.close()


if __name__=="__main__":
    main(sys.argv)
