# -*- coding: utf-8 -*-
'''
Created on Apr 12, 2013

@author: alexandre
'''



import time as t

import numpy as np
from jobDispatcher.mpi.async import AsyncFunc, dict_async, pool


def dotProfile( m,n,k, nRepeat ):
    """
    function for profiling matrix multiplications
    """
    a = np.random.rand(m,k)
    b = np.random.rand(k,n)
    
    t0 = t.time()
    for _i in range(nRepeat):
        _c = np.dot(a,b)
    dt = t.time()-t0
    
    nOp = m*n*k*nRepeat
    
    return nOp/dt


def dotProfileList(m,n,kMax,repeatFactor=1e7):
    
    """
    uses the mpiPool for submitting dotProfile for several values of k
    The results are collected in a dict_async.
    In the beginning, this dictionnary contains objects of type AsyncFunc.
    Then, progressively, it is replaced by the result. In the case of an Exception,
    the result will be an object of type Exception.
    """
    
    pool.start() # only mpi rank 0 will continue after this call
    
    result = dict_async()  
    for k in range(1,kMax):
        nRepeat = int(repeatFactor/(m*n*k))
        result[k] = AsyncFunc(dotProfile)( m,n,k, nRepeat )
    
    pool.join() # wait for all results to be computed
    return result

def plotResult(resD):
    """
    a simple function for plotting the result dictionnary
    """
    from matplotlib import pyplot as pp
    kL, tL = zip(*resD.items())
    pp.plot(kL, np.array(tL)/1e6)
    pp.ylabel('Mflops per core')
    pp.xlabel('k')
    pp.show()

if __name__ == "__main__":
    resD = dotProfileList(100,100,100)
    plotResult(resD)
#    print resD