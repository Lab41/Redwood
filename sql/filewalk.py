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
import mimetypes
import urllib

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


def write_stat_info(basename, dirname, file_id,  parent_id, dirname_digest, file_handle):
    path = dirname + '/' + basename
   
    try:
        stat_obj = os.stat(path)
    except Exception:
        #print "Error trying to stat {}".format(path)
        return

    url = urllib.pathname2url(path)
    file_type = mimetypes.guess_type(url)[0]
    hash_val = hash_file(path, file_type)
    data = {
            'hash':hash_val, 
            'dir_hash':dirname_digest, 
            'file_id':file_id, 
            'dirname':dirname, 
            'parent_id':parent_id, 
            'filename':basename, 
            'inode':stat_obj.st_ino, 
            'device':stat_obj.st_dev, 
            'permissions':str(oct(stat_obj.st_mode)), 
            'uid':stat_obj.st_uid, 
            'gid':stat_obj.st_gid, 
            'size':stat_obj.st_size, 
            'create_time':long(os.path.getctime(path)), 
            'access_time':long(stat_obj.st_atime), 
            'mod_time':long(stat_obj.st_mtime), 
            'metadata_change_time':long(stat_obj.st_ctime), 
            'user_flags':"", 
            'links':stat_obj.st_nlink, 
            'disk_offset':"", 
            'entropy':'', 
            'file_content_status':'', 
            'extension':os.path.splitext(basename)[1], 
            'file_type':file_type,
           }
  
    file_handle.write("{file_id:d},{parent_id:d},{dirname:s},{filename:s},{hash},"
            "{dir_hash:s},{inode:d},{device:d},{permissions:s},{uid:d},{gid:g},{size:d},"
            "{create_time:d},{access_time:d},{mod_time:d},{metadata_change_time:d},"
            "{user_flags:s},{links:d},{disk_offset},{entropy},"
            "{file_content_status},{extension:s},{file_type:s}\n".format(**data))


BUFFER = 4096

def hash_file(path, file_type):

    ret = ""
    #some files you can't hash
    if(file_type == 'inode/chardevice' \
            or file_type == 'inode/symlink' \
            or file_type == 'inode/socket' \
            or file_type == 'inode/blockdevice' \
            or file_type == 'inode/x-empty' \
            or file_type == 'application/x-coredump' \
            or file_type == 'inode/directory'):
        return ret
  
    fd = None
    try:
        h = hashlib.sha1()
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        data = os.read(fd, BUFFER)
        while(len(data)>0):
            h.update(data)
            data = os.read(fd, BUFFER)
        ret = h.hexdigest()
    except Exception, err:
        #print "Hash Error: {} on file {} with type {}".format(err, path, file_type)
        pass
    finally:
        if(fd != None):
            os.close(fd)
    return ret
    

omitted_dirs = ['dev', 'proc', 'sys']


def main(argv):

    if(len(argv) != 4):
        print "filewalk.py <directory> <os> <source>"
        return

    today = datetime.date.today()
    str_date = today.strftime('%Y-%m-%d')
    out_file = "{}--{}--{}".format(str_date, argv[2], argv[3]) 
    start_dir = argv[1]
    
    queue = Queue()

    with open(out_file, "w") as file_handle:
        
        file_handle.write("file_id, parent_id,dirname,basename,hash,fs_id,device,permissions,"
                "uid,gid,size,create_time,access_time,mod_time,metadata_change_time,"
                "user_flags,links,disk_offset,entropy,"
                "file_content_status,extensions,file_type\n")

        #start the queue with a 0 value
        queue.put(0L) 
         
        for root, dirs, files in os.walk(start_dir):
       
            parent_id = queue.get() 
            new_parent_id = generateUniqueId(root)
            try: 
                dirs.remove(omitted_dirs)
            except ValueError, e:
                pass

            for d in dirs:
                queue.put(new_parent_id)

            #optimization to skip proc fs 
            if(root.startswith('/proc') or root.startswith('/sys') or root.startswith('/dev')):
                continue
            
            h = hashlib.sha1()
            h.update(root)
            root_digest = h.hexdigest()

            #write the parent directory
            write_stat_info("/", root,  new_parent_id, parent_id, root_digest, file_handle)
            for f in files:
                _id = generateUniqueId(f)
                write_stat_info(f, root, _id, new_parent_id, root_digest, file_handle)
            file_handle.flush()
    file_handle.close()


if __name__=="__main__":
    main(sys.argv)
