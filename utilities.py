'''
sonicskye@2018

utilities.py contains general functions

'''
import hashlib
import datetime
import time

#@param BUFF_SIZE is buffer size to read arbitrary file
BUF_SIZE = 65536 #read stuff in 64KB chunks


def quotedstr(s):
    return '"%s"' % s


def singlequotedstr(s):
    return "'%s'" % s



# @dev getsha1string computes sha1 value from a string
# @param x is the string to compute
# @output sha1 value of x
"""
#compute SHA1 from a string
print (sha1string('ABCD'))
"""
def sha1string(x):
    h = hashlib.sha1()
    h.update(x.encode('utf-8'))
    return h.hexdigest()


# @dev getsha1file computes sha1 value from a file by reading it in chunks with the size of BUF_SIZE
# @param filepath is the path to the file
# @output sha1 value of the file
"""
#compute SHA1 from a file, the README.md
filepath = 'README.md'
print (sha1file(filepath))
"""
def sha1file(filepath):
    h = hashlib.sha1()
    with open(filepath,'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


# @dev functions related to time
# @dev two values are returned, ts in integer and st in string
# @dev ts is seconds after epoch (1 January 1970)
def gettimestamp():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return ts, st
