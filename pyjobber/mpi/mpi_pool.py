'''
Created on Jun 7, 2010

@author: alex
'''

from __future__ import with_statement

from threading import Thread, RLock 
import time
import sys
import traceback
from socket import gethostname
import atexit
from cPickle import dumps

ACTION_GET = 1
ACTION_OUT = 2
QUIT_SIGNAL = -2
NO_CALLBACK = -1

try:
    #Try importing MPI to dispatch evals/jobs across different cpus
    #fallback on _SinglePool if not available 
    import mpi4py.MPI as MPI #@UnresolvedImport
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()    
except:
    size = 1
    rank = 0
    print "WARNING : mpi4py not installed. Will run in single process."

isMain = rank == 0

verbose = False
class EmptyQueueD(Exception): pass

class QueueD:
    
    def __init__(self):
        self.d={}
        self.putId = 0
        self.getId = 0
        self.lock = RLock()
        
    def put(self, e):
        with self.lock:
            self.d[self.putId] = e
            self.putId+=1
        
    def get(self, _ignored=None):
        try:
            with self.lock:
                e= self.d.pop(self.getId)
                self.getId += 1
                return e
        except KeyError:
            raise EmptyQueueD()
        

def buildException():
    e = sys.exc_value
#    if len( e.args ) > 1:
#        eMsg = str(e.args)
#    else:
#        eMsg = str(e.args[0])
    e.args = ('occured on host %s with rank %d\n%s'%(gethostname(), rank, traceback.format_exc() ),)
    return e
    
    

def worker(memoizeFolder=None):
    
    while True:
        comm.send((rank, ACTION_GET, None), dest=0)
        if verbose : print '%d waiting get' % (rank)
        (taskId, f, args, kwArgs) = comm.recv(source=0)
        if taskId == QUIT_SIGNAL : break 
        
        if f is None: # nothing to do 
            out = None
            time.sleep(0.1) # ... just wait a little bit
        else:
            try: 
                if verbose : print 'Executing : %s on %d' % (str(f), rank)
                out = f(*args, **kwArgs)
#                print out
            except Exception:
                out = buildException()

        if taskId != NO_CALLBACK:
            if verbose : print 'Returning : %s from %d' % (str(f), rank)
            comm.send((rank, ACTION_OUT, (taskId, out)), dest=0)

if rank > 0:
    worker(None)
    sys.exit()

class mainLoop(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.setDaemon(True)
        self.queue = queue
        
    def run(self):
        queue = self.queue
        taskId = 0
        callbackDict = {}
        processes = size - 1 # number of active processes
        while True:
            
            if verbose : print 'listening ...'
            (dstRank, action, data) = comm.recv(source=MPI.ANY_SOURCE)
            if verbose : print 'Recieved action'
            if action == ACTION_GET:
                
                try:
                    task = queue.get(False)
                except EmptyQueueD:
                    task = (None, None, None, None, None) # Will make the worker slightly wait and comeback
                    
                if task is None:
                    if verbose : print 'Killing process %d' % dstRank
                    comm.send((QUIT_SIGNAL, None, None, None), dest=dstRank)
                    processes -= 1
                    
                else:
                    (f, args, kwArgs, callback, cbArgs) = task
                
                    if callback is not None:
                        callbackDict[taskId] = (callback, cbArgs)
                        if verbose : print 'Sending to %d : %s(*%s,**%s) using callback : %s' % (dstRank, str(f), str(args), str(kwArgs), str(callback))
    #                    strObj = dumps((taskId, f, args, kwArgs))
                        comm.send((taskId, f, args, kwArgs), dest=dstRank)
                        taskId += 1
                    else: 
                        comm.send((NO_CALLBACK, f, args, kwArgs), dest=dstRank)
                    
    
                                
            elif action == ACTION_OUT:
                (taskId_, out) = data
#                if isinstance(out,ExceptionInfo):
#                    sys.stderr.write(out.tb)
#                    comm.Abort(1)
                    
                (callback, cbArgs) = callbackDict[taskId_]
                if verbose : print 'Calling Back answer from %d with %s' % (dstRank, str(callback))
                callback(out,*cbArgs)
                if verbose : print 'Callback done'
                del callbackDict[taskId_]
    
            if processes == 0:
                break


class TimeoutError(Exception): pass

class ApplyResult:
    """
    This mimics multiprocessing.pool.ApplyResult's behavior.
    """
    
    def __init__(self, callback=None, cbArgs=()):
        self._callback = callback
        self._cbArgs = cbArgs
        self._ready = False
    
    def _set(self, value):
        self._value = value
        self._success = not isinstance(value, Exception)
        self._ready = True

        if self._callback is not None and self._success:
            self._callback( value, *self._cbArgs )    
      
    def ready(self):
        return self._ready

    def successful(self):
        assert self._ready
        return self._success

    def wait(self, timeout=None):
        if self._ready: return
        
        t0= time.time()
        while True:
            if self._ready:
                break
            
            if timeout is not None:
                if time.time() - t0 > timeout :
                    break
            else:
                time.sleep(0.001)
        

    def get(self, timeout=None):
        self.wait(timeout)
        if not self._ready:
            raise TimeoutError
        if self._success:
            return self._value
        else:
            raise self._value

#
class _Pool:
    
    def __init__(self):
        self.queue = QueueD()
        self.size = size
        
        if verbose : print 'Starting mpiPool with %d process'%(size)
        self.mainLoop = mainLoop(self.queue)
        self.mainLoop.start()
        atexit.register(self.stop)
    
    def __str__(self):
        return 'MpiPool of size %d'%self.size 
       
        
        
    def apply_async(self, f, args=(), kwArgs={}, callback=None, cbArgs=()):
        
        # if these objects are not picklable, this will raise the 
        # error in the main thread instead of later in the queue thread
        _dataStr = dumps( (f,args, kwArgs) )    
                                                
        apply_result = ApplyResult( callback, cbArgs )
        self.queue.put( (f, args, kwArgs, apply_result._set, () ) )
        return apply_result

    
    def join(self):
        self.stop()


    def close(self):
        return self.stop()
        
    def stop(self):
        if isMain:
            for _i in xrange(size-1):
                self.queue.put(None) # message for stopping a worker
            self.mainLoop.join()



class _SinglePool:
    """
    in a non mpi environement, this allows to continue with the normal execution flow.
    """
    
    def __init__(self):
        # useless, but in case something wants to access this variable
        self.queue = QueueD() 
        self.size = size
        self.isMain = isMain
        
    
    def apply_async(self, f, args=(), kwArgs={}, callback=None, cbArgs=()):
        out = f(*args, **kwArgs) # apply function now instead of deferred
        
        apply_result = ApplyResult( callback, cbArgs )
        apply_result._set(out)
        return apply_result

    def stop(self):
        pass
    
    def join(self):
        self.stop()
    
#    def start(self,memoizeFolder=None):
#        return True

if size == 1:
    pool = _SinglePool()
else:
    pool = _Pool()


def Pool():
    return pool
