import sys
import subprocess
import datetime
import hashlib
import time
import struct 
import binascii
import os
import re

def walker(file_handle, dirname, dirs, fnames):

    #optimization to skip proc fs 
    if(dirname.startswith('/proc')):
        return

    m = hashlib.md5()
    #dirname = dirname.rstrip("/")
    m.update(dirname)
    #gives us 16 bytes, which we will take 5 of
    digest = m.digest()
    first_five = digest[:5]
    #take th first 3 bytes of the time
    packed_time = struct.pack("I", int(time.time()))
    combined = digest[:5] + packed_time[:3]
    index = long(binascii.hexlify(combined), 16) 

    merged_names = fnames + dirs

    for f in merged_names:
        path = dirname + '/' + f
        try:
            stat_obj = os.stat(path)
        except Exception:
            print "Error trying to stat {}".format(path)
            continue 

        p = subprocess.Popen(['file', '-b', '--mime-type', path], stdout=subprocess.PIPE)
        p.wait()
        file_type = p.stdout.read().rstrip()#.lstrip('inode/')
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
                'dirname':dirname, \
                'parent_id':index, \
                'filename':f, \
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
                'extension':os.path.splitext(f)[1], \
                'file_type':file_type,
               }

        file_handle.write("{hash},{dirname:s},{parent_id:d},{filename:s},"
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
        print "filewalk.py <directory> <output_file>"
        return

    with open(argv[2], "w") as f:
        #os.walk(argv[1], walk_callback, f)
        for root, dirs, files in os.walk(argv[1]):
            walker(f, root, dirs, files)


    f.close()


if __name__=="__main__":
    main(sys.argv)
