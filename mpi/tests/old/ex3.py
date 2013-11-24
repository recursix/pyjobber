'''
Created on Jun 7, 2010

@author: alex
'''

print 'import'
from pyon.defer import RFun, DFun, pool
import numpy as np
import time as t
print 'Done import'


def sub(n):
    x = np.arange(float(n))/n
    A = np.outer(x,x)
    B = np.dot(A,A)
#    t.sleep(0.1)
    return np.dot(np.dot(x,B),x)


def f_D(n,k):
    resL = [ RFun(sub)(n+i)  for i in xrange(k) ]
    return DFun(merge)(*resL)

def f(n,k):
    resL = [ sub(n+i)  for i in xrange(k) ]
    return merge(*resL)

def merge(*argL):
    return sum(argL)
        
        
if __name__ == "__main__":
    
    if pool.start():
    
        n = 100
        k = 10
        
        print 'compiling ...'
        res_D = f_D(n,k)
        print 'compiled'
        
        res = f(n,k)
        
        assert res_D.wait() == res
        
        print 'computed'
        
        

#pool.stop()


