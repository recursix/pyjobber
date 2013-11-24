# -*- coding: utf-8 -*-
'''
Created on 2012-07-05

@author: alexandre
'''
from __future__ import with_statement


import time as t
from os import path
from os.path import join

wd = path.abspath('.')
jobStatePath = join( wd, 'jobState' )

# write the running timestamp in jobState
with open( jobStatePath, 'a' ) as jobState:
    jobState.write( 'running %s\n'%t.strftime('%d%b%Y-%H:%M:%S') )



import subprocess as sp
import sys
import os
import gzip
import cPickle
import signal
import thread
from jobDispatcher import dispatch

# gather some paths
stdoutPath = join( wd, 'stdout' )
stderrPath = join( wd, 'stderr' )
runPath = join( wd, 'run' )
callablePath = join( wd, 'callable.pkl' )
#runCallablePath = os.environ['RUN_CALLABLE']
runCallablePath = dispatch.runCallablePath
qsubrcPath = path.expandvars( join( "$HOME", '.qsubrc' ) )




def readPkl( *args ):
    with open( path.join( *args ), 'r') as fd: 
        return cPickle.load(fd)

def dieWithParent(pid, delay=0.1, sig=signal.SIGTERM):
    while True:
        t.sleep(delay)
        if os.getppid() == 1:
            print 'exiting %d'%os.getpid()
            os.kill(pid, sig)
            break

def findNcpu(nCpu,nNode,ppn):
    if nCpu is not None: return nCpu
    if nNode is None : nNode = 1
    
    if ppn is None:
        try : ppn = int( os.environ['JD_PPN'] ) 
        except ValueError : pass # not an integer
        except KeyError   : pass # not defined
    if ppn is None:
        import multiprocessing
        ppn = multiprocessing.cpu_count()
        print "Warning, unknown number of processor per node (ppn). Multiprocessing is reporting %d"%ppn 
    
    return nNode*ppn

jobConf = readPkl(wd,'conf.pkl')

argL = []


# add mpirun if requested
if jobConf.mpi:
    nCpu = findNcpu(jobConf.nCpu, jobConf.nNode, jobConf.ppn)
    if nCpu > 1 :
        argL += ['mpirun', '-np', str(jobConf.nCpu) ]
    if jobConf.splitStdout:
        argL += ['--output-filename', 'stdout' ]


# will execute either "run" or "python runCallable.py"
if path.exists( runPath ):
    argL.append( runPath )
elif path.exists( callablePath ):
    argL += [ sys.executable, runCallablePath ]
else:
    raise Exception('Nothing to run for job %s.'%wd )        


    
os.chdir(wd) # makes sure that the qsubrc didn't changed the current working directory



# run the program
print ' '.join( argL )
with open( stdoutPath, 'a+' ) as stdout: 
    with  open( stderrPath, 'a+' ) as stderr:
        p = sp.Popen( argL, stdout=stdout, stderr=stderr,  cwd=wd )
        thread.start_new_thread(dieWithParent,(p.pid,))
        p.wait()

# write the done timestamp in jobState
with open( jobStatePath, 'a' ) as jobState:
    jobState.write( 'done %s\n'%t.strftime('%d%b%Y-%H:%M:%S') )

def compressFile(filePath):
    gzPath = filePath + '.gz' 
    gz = gzip.open( gzPath  , 'wb' )
    with  open( filePath, 'rb' ) as fd:
        gz.write(fd.read())
    gz.close()

# compress stdout to stdout.gz and remove stdout
# also do it for all mpi stdouts
if jobConf.compressStdout:
    for fn in os.listdir(wd):
        if fn.startswith('stdout') or fn.startswith('stderr'):
            filePath = join(wd,fn)
            if path.getsize(filePath) > 0 :
                compressFile(filePath)
                
            try: os.remove( filePath ) 
            except : pass # may be locked by another process


sys.exit()