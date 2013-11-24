'''
Created on Jun 6, 2010

@author: alex
'''
from __future__ import with_statement
from jobDispatcher.mpi.mpiPool import pool
import sys
import traceback
from threading import RLock, Thread
import time
import graalUtil.memoize as pyUtil
import graalUtil.file as fileUtil
import os
from os import path
verbosity = 0


def filterCallbackList( cbL ):
    cbL_ = []
    for cb, argL, link in cbL:
        if link:
            cbL_.append( ( pyUtil.wrapMethod( cb ), argL, link ) )
        
    return cbL_
    
    
class Deferred:
    
    def __init__(self):
        self._cbL = []
        self._pcbL = []
        self.waiting = 0 # number of dependencies this object is waiting for
        self.lock = RLock()
        
    def isComputed(self): return self.waiting == 0
        
    def addComputedCallback(self, cb, args=(), link=False ):
        if self.isComputed(): cb(self.getVal(),*args) # already computed. Calling back immediately.
        else:
            with self.lock: self._cbL.append( (cb, args, link) )

    def addProgressCallback(self, cb, args=(), link=False ):

#        print '%s add progress callback (%s), %d callback, waiting:%d'%(self.__class__.__name__, cb.func_name, len(self._pcbL),self.waiting )
        if self.isComputed(): cb(self.getVal(),0,*args) # already computed. Calling back immediately.
        else:
            with self.lock: self._pcbL.append( (cb, args, link) )
    
    def _computedCallback(self, val):
        self._progressCallback(val, 0)

        if verbosity > 0 : 
            if hasattr(self,'name'):    
                sys.stderr.write( 'computed cb of %s \n'%self.name)
        
        for (cb,args,_link) in self._cbL : 
            cb( val, *args )

    def _progressCallback(self, val, waitingCount):
        
#        if verbosity > 0 :
#            if hasattr(self,'name'): 
#                sys.stderr.write( 'progress cb of %s (waiting = %d, %d)\n'%(self.name, waitingCount, self.waiting) )

#        print '%s progress callback, %d callback, waiting:%d'%(self.__class__.__name__,len(self._pcbL),self.waiting )
        for (cb,args,_link) in self._pcbL: cb(val, waitingCount, *args)

    def __getstate__(self):
        odict = self.__dict__.copy()
        # Disconnect callbacks which are not links. 
        # To avoid pickling error, wrapMethod will encapsulate the callback if it is an instancemethod  
        odict['_cbL']  = filterCallbackList( self._cbL  )  
        odict['_pcbL'] = filterCallbackList( self._pcbL )
        del odict['lock']
        return odict
    
    def __setstate__(self,dict_):
        self.__dict__.update(dict_)
        self.lock = RLock()
        
    def wait(self):
        while self.waiting > 0: time.sleep(0.001)
        return self


def isComputed( var ):
    if isinstance(var, Deferred): return var.isComputed()
    else: return True

def join( var ):
    if isinstance(var, Deferred): return var.wait()
    else : return var
    
def getAnswer(obj, default=None):
    if not isinstance(obj, Question):
        return obj
    
    answer = getattr(obj, 'answer',default)
    if isinstance( answer, Exception ):
        raise answer
    return answer
           
    
class DDict(Deferred):
    
    def __init__(self):
        Deferred.__init__(self)
        self.d = {}
    
    def __len__(self):      
        return self.d.__len__()
    
    def keys(self):         
        return self.d.keys()
    
    def has_key(self,key):  
        return self.d.has_key(key)
           
    def _itemset(self, x, key ):
        with self.lock:
            self.d[key] = x 
            self.waiting -= 1
        if self.waiting == 0:
            self._computedCallback(self)

    def getVal(self): return self

    def getName(self):
        if hasattr(self,'name_'):
            return self.name_
        else:
            strL = [str(e) for e in self.d.keys() ]
            return ', '.join(strL)
        
    name = property(getName)

    def wake(self):
        for val in self.itervalues():
            if isinstance(val,Deferred): 
                val.wake()
        
    
    def __setitem__(self, key, x ):
        with self.lock:
            self.d[key] = x 
            if isinstance( x,Deferred):
                self.waiting += 1
                x.addComputedCallback(self._itemset, (key,), link=True)
                x.addProgressCallback(self._progressCallback,  link=True)

    
    def __getitem__(self,key):
        x = self.d[key]
        # exceptions are raised at access time
        if isinstance( x, Exception ): raise x
        return x

    def itervalues(self):
        for x in self.d.itervalues():
            # exceptions are raised at access time
            if isinstance(x,Exception): raise x 
            else : yield x
    
    def iteritems(self):
        for key, x in self.d.iteritems():
            # exceptions are raised at access time
            if isinstance(x,Exception): raise x
            else : yield key,x
            
#    def clearException(self):
#        for key, x in self.d.items():
#            if isinstance(x,Exception):
                

    
    def __str__ (self): return self.d.__str__ ()
    def __repr__(self): return self.d.__repr__()


def hasDeferred( valL ):
    return any([ isinstance(val, Deferred) for val in valL ])

class ArgDD( DDict ):
    
    def __init__(self, argL, argD, funcName=None ):
        DDict.__init__(self)
        if funcName is not None : self.name_ = 'args of %s'%funcName
        self.nArgs = len(argL) # will be used when extracting arguments from argD
        for i,  arg in enumerate(argL):  self[i]   = arg
        for key,arg in argD.iteritems(): self[key] = arg
        
    def extract(self):
        argL = [None]*self.nArgs
        argD = {}
        for key, arg in self.iteritems():
            if isinstance(key, int): argL[key] = arg
            else:                    argD[key] = arg
        
        return argL, argD 
    
    
class Question(Deferred):
    """
    A question, is an object that contains everything required to eventually obtain the answer.
    If there is an arg that is deferred, it will wait for it to be computed before calling f.
    If remote is True, it will be executed on the mpiQueue.
    """
    def __init__(self, f, argL, argD, remote=False):
        Deferred.__init__(self)
        self.waiting = 1
        self.f = f
        
        if   hasattr(f, '__name__'):  self.name = f.__name__
        elif hasattr(f, '__class__'):  self.name = f.__class__.__name__
        else: self.name = 'unknown'
        
        self.remote = remote
        
        if hasDeferred(argL) or hasDeferred( argD.itervalues() ):
            # accumulate arguments in a deferred structure
            self.argDD = ArgDD( argL, argD, self.name )
            
            # self._preCall will be called when argD will be computed (i.e. when all arguments are computed)
            # if there were no deferred arguments, it will be called immediately (in the same thread)
            self.argDD.addProgressCallback( self._progressCallback,  link=True)
            self.argDD.addComputedCallback( self._preCall, link=True )
        else:
            self.argL = argL
            self.argD = argD
            self._call( )
                 
        
    def _preCall(self, argDD ):
        """This function is called when all arguments in self.argDD are computed"""
        self.argL, self.argD = argDD.extract()
        self.argDD=None
        self._call( )

    def _call( self ):
        if self.remote:
            self.job = pool.apply_async( self.f, self.argL, self.argD , self._return )
        else:
            try:     
                ans = self.f( *self.argL, **self.argD)
            except :
                ans = sys.exc_value 
                ans.args =  (traceback.format_exc(),)
            self._return( ans )
            
    def _return(self,answer):
        if verbosity > 0 : print 'Returning with lock ...'
        with self.lock:
            self.answer = answer
            self.waiting = 0
        self._computedCallback(self.answer)
    
    def getVal(self): return self.answer
    
    def preview(self):
        """
        Tries to extract a preview if not already computed
        """
        if self.isComputed():
            if isinstance(self.answer, Exception ):
                raise self.answer
            return self.answer
        else:
            if hasattr(self, 'argD') and hasattr(self,'argL'): # ready to be called
                return self.f( *self.argL, **self.argD )
            elif hasattr(self, 'argDD' ): 
                argL, argD =  self.argDD.extract()
                return self.f( *argL, **argD )
            else:
                raise Exception('Question is in a unknown state')
    
    def wake(self): 
        if not self.isComputed() or isinstance(self.answer, Exception): # else, already computed, or got Exception, try again
            if hasattr(self, 'argD') and hasattr(self,'argL'): # ready to be called
                self._call()
            elif hasattr(self, 'argDD' ):  # else, DFun have never been called
                self.argDD.wake() # propagate the wake signal to all arguments
#                if self.argDD.isComputed(): # else _preCall should be in the callback list of argD
#                    self._preCall(self.argDD)
            else:
                raise Exception('Question is in a unknown state')
            
        
    def wait(self):
        Deferred.wait(self)
        return self.answer
    



class DFun:

    def __init__(self,f,fanOut=1):
        """
        f is the function to be executed
        """
        self.f = f
        self.remote = False
        self.fanOut = fanOut

    def __call__(self, *argL, **argD):
        q = Question( self.f, argL, argD, remote=self.remote )
        if self.fanOut == 1:
            return q
        else:
            return [ fanOut( q, i ) for i in range(self.fanOut) ]

def fanOut_(x, i ): return x[i]
fanOut = DFun(fanOut_)

    
class RFun(DFun):

    def __init__(self,f,fanOut=1):
        DFun.__init__(self, f, fanOut)
        self.remote = True



class Bkp(Thread):
    
    def __init__(self,f,bkpPath,minDelay=10,delBkp=False,incrBkp=False):
        Thread.__init__(self)
        
        self.f = f
        self.bkpPath = bkpPath
        self.bkpPathTmp = bkpPath+'-tmp'
        self.minDelay = minDelay # will wait at least minDelay between two backup
        self.delBkp=delBkp
        self.incrBkp = incrBkp
        
        self.lastBackup = 0
        self.needToBackup = False
        self.done = False
        self.incr = 0
        
        
    def __call__( self, *argL, **argD ):
       
        try : # try to load a backup
            self.dObj = fileUtil.readPklz( self.bkpPath )
            self.dObj.wake()
        except (EOFError, IOError): # if no backup, start the program from scratch
            self.dObj = self.f( *argL, **argD )
        
        
        if isinstance( self.dObj, Deferred):
            
            self.start()
            self.dObj.addProgressCallback(self.flagBackup)
            self.dObj.addComputedCallback(self.stop)
            
        return self.dObj
        
            
    def run(self):
        while not self.done:
            if self.needToBackup:
                if time.time() > self.lastBackup + self.minDelay:
                    self.needToBackup = False
                    self.backup()
                    
            
            time.sleep(0.01)
        self.backup()

    def flagBackup(self,*args):
        self.needToBackup = True
        
        
    def backup(self):
        # Writing backup in two steps. 
        # This tries to avoid being killed while overwriting an existing backup
        
        print 'writing bkp %s'%self.bkpPath
        fileUtil.writePklz(self.dObj, self.bkpPathTmp)
        
        if self.incrBkp:
            p,ext = path.splitext(self.bkpPath)
            bkpPath = p + str(self.incr) + ext
            self.incr +=1
        else:
            bkpPath = self.bkpPath
            
        os.rename(self.bkpPathTmp, bkpPath) 
        self.lastBackup = time.time()
        
            
    def stop(self,*_):
        self.done = True
        self.join()
        if self.delBkp and path.exists( self.bkpPath ):
            os.remove(self.bkpPath)
        if path.exists( self.bkpPathTmp ):
            os.remove(self.bkpPathTmp)


    


#def _identity(f):
#    return f
#
#if pool.size == 1: 
#    # in this case, remote execution is useless
#    RFun = DFun 




