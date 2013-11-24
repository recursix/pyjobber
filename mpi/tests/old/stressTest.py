#!/usr/bin/env python
'''
Created on Jul 18, 2010

@author: alex
'''


from random import randint
import shellUtil
import time as t
import sys
from os import path
import os

logFolder = path.expandvars('$HOME/data/log')
idx = 0
while True:
    npL = [1,2]
    np = npL[ randint(0,len(npL)-1) ]
    
    cmd = 'mpirun -np %d python ex2.py'%(np)
    
    
    sys.stdout.write( 'run %4d with %2d processes ... '%(idx,np) )
    sys.stdout.flush()
    t0 = t.time()
    stdoutPath = path.join( logFolder, 'pyon-stresstest-%s-%d (np=%d).stdout'%(t.ctime(), idx,np ) )
    stderrPath = path.join( logFolder, 'pyon-stresstest-%s-%d (np=%d).stderr'%(t.ctime(), idx,np ) )
        
    p = shellUtil.timedCall( 2*60, cmd.split() , stdout=open(stdoutPath, 'w'), stderr=open(stderrPath, 'w') )
    p.wait()
    et = t.time() - t0
    
    msg = 'took %.3fs'%(et)
    
    if p.returncode != 0:
        print msg + ' : error'
    else:
        print msg + ' : ok'
        os.remove(stdoutPath)
        os.remove(stderrPath)
    idx+=1 
    
