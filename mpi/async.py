# -*- coding: utf-8 -*-
'''
Created on Apr 12, 2013

@author: alexandre
'''



#from jobDispatcher.mpi.mpiPool import pool
from multiprocessing import Pool
import sys
from traceback import format_exc
from threading import RLock

pool = Pool()

def print_exception(exception):
    try:
        raise exception # just raise and catch it so format_exc print the right thing
    except:
        sys.stderr.write( format_exc() )
    
class ReturnResult:

    def __init__(self, obj, key ):
        self.obj = obj
        self.key = key
        
    def __call__(self, res ):
        
        if isinstance(res, Exception):
            print_exception(res)
        else:
            self.obj._return_result(self.key, res)

class AsyncFunc:
    """
    Encapsulate a function for asynchrone execution.
    
    def square(x) : return x**2
    square_async = AsyncFunc(square)
    """
    
    def __init__(self, f):
        self.f = f
    
    def __call__(self, *argL, **argD ):
        self.argL = argL
        self.argD = argD
        return self
    
    def enqueue(self,cb):
        pool.apply_async( self.f, self.argL, self.argD, cb )

    
AF = AsyncFunc # a shortcut

class CallbackList:

    def __init__(self):
        self.cbD = {}
        self.id = 0
        
    def add(self, f, *argL, **argD):
        self.id += 1
        self.cbD[self.id] =  (f,argL, argD) 
        return self.id 
    
    def remove(self, callback_id):
        del self.cbD[callback_id]
    
    def callback(self):
        for f,argL,argD in self.cbD.values():
            f(*argL, **argD)

class CallbackList_arg:

    def __init__(self):
        self.cbD = {}
        self.id = 0
        
    def add(self, f ):
        self.id += 1
        self.cbD[self.id] = f # todo make thread safe
        return self.id 
    
    def remove(self, callback_id):
        del self.cbD[callback_id] # todo make thread safe
    
    def callback(self,*argL, **argD):
        for f in self.cbD.values():
            f(*argL, **argD)


#class PicklableMethod:
#    """Render instanceMethod picklable"""
#    def __init__(self, im):
#        assert isinstance(im, types.MethodType )
#        self.obj = im.im_self
#        self.methodName = im.func_name
#            
#    def __call__(self, *args, **kwargs):
#        return getattr(self.obj, self.methodName)(*args, **kwargs)
#
#def wrapMethod( f ):
#    if isinstance(f, types.MethodType ): 
#        return PicklableMethod(f)
#    else:
#        return f

class AsyncStruct(object):
    
    def __init__(self):
        self._pending = {}
        self.lock = RLock()
        self.completed_callback = CallbackList()
        self.assign_callback = CallbackList_arg()
        
    def _return_result(self, key, val):
        self._assign_result(key, val)
        with self.lock:
            del self._pending[key]
        self.assign_callback.callback(key,val)
        
        if len(self._pending) == 0:
            self.completed_callback.callback()

    def pending_count(self):
        return len(self._pending)

    def _assign_result(self, key, val):
        raise NotImplementedError()

    def _set_async(self, key, val):
        if isinstance( val, AsyncFunc ):
            # TODO : what if it is already assigned ?
            with self.lock:
                self._pending[key] = val
            val.enqueue(ReturnResult( self, key ))

        else:
            self._assign_result(key, val)

    def _sub_completed(self, key):
        del self._to_wake[key]

class AsyncDict(AsyncStruct,dict):

    def __setitem__(self, key, val):
        self._set_async(key,val)

    def _assign_result(self, key,val):
        with self.lock:
            dict.__setitem__(self,key,val)

#class AsyncDictBkp(AsyncStruct,dict):
#
#    def __init__(self,bkp_path):
#        super(AsyncStruct,self).__init__()
#        super(dict,self).__init__()
#        self.bkp_path = bkp_path
#
#    def __setitem__(self, key, val):
#        self._set_async(key,val)
#
#    def _assign_result(self, key,val):
#        dict.__setitem__(self,key,val)

class AsyncObj(AsyncStruct):
    
    def __setattr__(self, key,val):      
        self._set_async(key,val)
    
    def _assign_result(self, key,val):
        with self.lock:
            self.__dict__[key] = val


def test_object_async():
    
    class MyAssync(AsyncObj):
        
        def __init__(self,c=0):
            super(MyAssync,self).__init__()
            self.c = c
    
    a=MyAssync(5)
    

    a.b = 3
    print a.b
    print a.c
    print a._pending

if __name__ == "__main__":
    test_object_async()
    