'''
Created on Jun 6, 2010

@author: alex
'''

import pyon.engine as pyon
from pyon.tools import F, CB

import time as t
import numpy as np
import matplotlib.pyplot as pp


def dotN( n ):
    """Function to be remotely called"""
    a = np.random.rand(n,n)
    t1 = t.time()
    np.dot(a,a)
    t2 = t.time()
    return t2-t1
        

if pyon.main:
    
    nL = range(10,1000,10)
#    nL = [500]*10
    tL = [None]*len(nL)
    
    def callback(dt,i):
        """this function is called each time a result is finished"""
        tL[i] = dt
        
    def benchDot( nL ):
        """Iterator of tasks"""
        for i,n in enumerate(nL):
            yield F( dotN, n ), CB( callback, i ) # equivalent to : callback( dotN(n), i ) but dotN(n) is remotely executed

    t1 = t.time()
    pyon.exe( benchDot( nL ) ) 
    t2 = t.time()
    
    nOp = sum( [ n**3 for n in nL ] )
    gflops = float(nOp) / (t2-t1) / 1e9
    print 'total gflops : %.3g'%gflops 
    
    gflopsL = [ (n**3 / t ) / 1e9 for n,t in zip(nL,tL)]
    
    pp.plot(nL, gflopsL )
    pp.show()

        
else: 
    pyon.worker()


#print 'done : %d/%d'%(pyon.rank, pyon.size)
