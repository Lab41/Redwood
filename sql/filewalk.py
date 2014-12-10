import binascii
import datetime
import hashlib
import mimetypes
import os
import re
import struct
import subprocess
import sys
import time
import urllib
import csv
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


def write_stat_info(basename, dirname, file_id, parent_id, dirname_digest, csv_writer):
    
    #need to escape commas from base name and dirname since we are creating a csv

    
    path = os.path.join(dirname, basename)

    try:
        stat_obj = os.stat(path)
    except Exception:
        # print "Error trying to stat {}".format(path)
        return

    url = urllib.pathname2url(path)
    file_type = mimetypes.guess_type(url)[0]
    hash_val = hash_file(path, file_type)

#file_id, parent_id,dirname,basename,hash,fs_id,device,permissions,uid,gid,size,create_time,access_time,mod_time,metadata_change_time,user_flags,links,disk_offset,entropy,file_content_status,extensions,file_type

    csv_writer.writerow([file_id, parent_id, dirname, basename, hash_val, dirname_digest, stat_obj.st_ino, stat_obj.st_dev,
                        str(oct(stat_obj.st_mode)), stat_obj.st_uid, stat_obj.st_gid, stat_obj.st_size, long(os.path.getctime(path)),
                        long(stat_obj.st_atime), long(stat_obj.st_mtime), long(stat_obj.st_ctime), "", stat_obj.st_nlink, "", "", "",
                        os.path.splitext(basename)[1], file_type])


BUFFER = 4096

def hash_file(path, file_type):

    ret = ""
    # some files you can't hash
    if(file_type == 'inode/chardevice' \
            or file_type == 'inode/symlink' \
            or file_type == 'inode/socket' \
            or file_type == 'inode/blockdevice' \
            or file_type == 'inode/x-empty' \
            or file_type == 'application/x-coredump' \
            or file_type == 'inode/directory'):
        ret = "0"
        return ret

    fd = None
    try:
        h = hashlib.sha1()
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NONBLOCK', 0) | os.O_NONBLOCK)
        data = os.read(fd, BUFFER)
        while(len(data)>0):
            h.update(data)
            data = os.read(fd, BUFFER)
        ret = h.hexdigest()
    except Exception, err:
        # print "Hash Error: {} on file {} with type {}".format(err, path,
        # file_type)
        pass
    finally:
        if(fd != None):
            os.close(fd)
    return ret


omitted_dirs = ['/dev', '/proc', '/sys', '/Volumes', '/mnt', '/net']


def main(argv):

    if(len(argv) != 5):
        print "filewalk.py <directory> <os> <source> <output_dir>"
        return


    #make sure output dir exists
    if os.path.exists(argv[4]) is False:
        print "Output dir {} does not exist".format(argv[4])
        return

    today = datetime.date.today()
    str_date = today.strftime('%Y-%m-%d')
    out_file = os.path.join(argv[4], "{}--{}--{}".format(str_date, argv[2], argv[3]))
    start_dir = argv[1]


    stack = list()

    with open(out_file, "w") as file_handle:
        
        csv_writer = csv.writer(file_handle)
        csv_writer.writerow(["file_id","parent_id","dirname","basename","contents_hash", "dirname_hash", "fs_id","device","permissions",
                "uid","gid","size","create_time","access_time","mod_time","metadata_change_time",
                "user_flags","links","disk_offset","entropy","file_content_status","extensions","file_type"])

        # start the queue with a 0 value
        stack.append(0L)

        for root, dirs, files in os.walk(start_dir):
            # We want to have a nice, dynamic output that doesn't flood the
            # terminal with lines of text. So we'll write a line, then flush it
            # with '\r'. In order to do this properly, we need to first measure
            # the width of the terminal.
            # We're also going to put it inside the loop in case the window
            # gets resized while it's running
            rows,columns = os.popen('stty size', 'r').read().split()
            rows = int(rows)
            columns = int(columns)

            parent_id = stack.pop()

            #some directories we will ignore as so
            if root in omitted_dirs:
                del dirs[:]
                continue

            sys.stdout.write('\r')
            sys.stdout.write(' ' * columns)
            sys.stdout.write('\r')
            sys.stdout.write('processing {}'.format(root[:columns-12]))
            sys.stdout.flush()

            new_parent_id = generateUniqueId(root)

            # for each of the child dirs, add the parent id. This assumes a BFS
            # search
            for d in dirs:
                stack.append(new_parent_id)

            h = hashlib.sha1()
            h.update(root)
            root_digest = h.hexdigest()

            # write the parent directory
            write_stat_info("/", root,  new_parent_id, parent_id, root_digest,csv_writer)
            for f in files:
                _id = generateUniqueId(os.path.join(root, f))
                write_stat_info(f, root, _id, new_parent_id, root_digest, csv_writer)
            file_handle.flush()

if __name__=="__main__":
    main(sys.argv)
