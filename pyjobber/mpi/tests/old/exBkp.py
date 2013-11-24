'''
Created on Aug 16, 2010

@author: alex
'''

'''
Created on Jun 7, 2010

@author: alex
'''

from pyon.defer import RFun, DFun 
import numpy as np
import time as t



def sub(n):
    x = np.arange(float(n))/n
    A = np.outer(x,x)
    B = np.dot(A,A)
#    t.sleep(0.5)
    return np.dot(np.dot(x,B),x)



def f_D(n,k):
    resL = [ RFun(sub)(n+i)  for i in xrange(k) ]
    return DFun(merge)(*resL)

def f(n,k):
    resL = [ sub(n+i)  for i in xrange(k) ]
    return merge(*resL)

def merge(*argL):
    return sum(argL)
        

    
    

#pool.stop()


