# -*- coding: utf-8 -*-
'''
Created on 2012-06-20

@author: alexandre
'''
from __future__ import with_statement
from os import path
import cPickle
import gzip


def replaceD( str_, rDict):
    """
    Inefficient way of replacing characters in a string
    """
    for key,val in rDict.items():
        str_ = str_.replace( key, val ) 
    return str_

def writeFile( data, *args ):
    """
    A wrapper to simplify file writing
    fileName = path.join(*args)
    """
    filePath = path.join( *args )
    with open( filePath, 'w' ) as fd:
        fd.write(data) 

  
                    
def readFile( *args ):
    """
    A wrapper to simplify file reading
    fileName = path.join(*args)
    If fileName doesn't exist None is returned.
    """
    filePath = path.join( *args )
    if not path.exists( filePath ):
        return None
    with open( filePath, 'r' ) as fd:
        return fd.read() 
    
def readFileGz( *args ):
    filePath = path.join( *args )
    if not path.exists( filePath ):
        return None
    with gzip.open( filePath, 'r' ) as fd:
        return fd.read()   


def writePkl( obj, *args, **kwArgs ):
    """
    Serialize an object to file.
    the fileName is path.join(*args)
    """
    pklPath = path.join( *args )
    with open( pklPath, 'w' ) as fd:            
        cPickle.dump( obj, fd, cPickle.HIGHEST_PROTOCOL )


#def writePklz( obj, *args ):
#    writeFile( zlib.compress( cPickle.dumps(obj,cPickle.HIGHEST_PROTOCOL) ), *args )
#
#def readPklz( *args ):
#    with open( path.join(*args), 'r' ) as fd:
#        data = fd.read() 
#    return cPickle.loads(zlib.decompress( data ))
#        
def readPkl( *args, **kwArgs ):
    """
    Unserialize an object from file.
    the fileName is path.join(*args)
    """
    try:
        with open( path.join( *args ), 'r') as fd: return cPickle.load(fd)
    except IOError: 
        if kwArgs.has_key('defaultVal'): return kwArgs['defaultVal']
        else: raise

minute = 60
hour = 60*minute
day = 24*hour

def formatTimeStr( timeStr ):
    tL = timeStr.split(':')
    tL.reverse()
    s = 0
    for amount, factor in zip( tL, (1,minute,hour,day) ):
        s += int(amount)*factor
    return s


class Callable:
    
    def __init__(self, f, argL, argD ):
        self.f = f
        self.argL = argL
        self.argD = argD
        
    def __call__(self):
        return self.f( *self.argL, **self.argD )


