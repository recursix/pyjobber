'''
Created on Aug 16, 2010

@author: alex
'''

from pyon.defer import  pool, Bkp
from pyon.tests.exBkp import f_D 


if pool.start():

    n = 500
    k = 10
    
    print 'compiling ...'
    res = Bkp(f_D,'bkp/exBkp.pklz',0,False,True)(n,k).wait() 
    
    print res
    
    print 'computed'
