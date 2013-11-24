'''
Created on Jun 7, 2010

@author: alex
'''


from pyon.defer import DDict, RFun, join, pool
import numpy as np
import time as t

def f1(n):
    x = np.arange(float(n))/n
    A = np.outer(x,x)
    B = np.dot(A,A)
    t.sleep(0.1)
    return np.dot(x,B)

R_f1 = RFun(f1)

def f2(x):
    return x.mean()

def f(n,k):
    res = DDict()
    
    for i in xrange(k):
        res[i] = R_f1(n+i) 
    
    return res

    
    

if pool.start():

    n = 100
    k = 10
    
    res_D = f(n,k) # we must be aware that the result of f is deferred.
#
    
    while res_D.waiting > 0:
        print 'waiting : %d'%res_D.waiting
        t.sleep(0.02)
    print 'done waiting'
    
    for i in xrange(k):
        
        m = f2( join( res_D[i] ) ) 
#        print m  
        m_ = f2( f1(n+i) )
        assert m == m_
    

#pool.stop()


