# -*- coding: utf-8 -*-
'''
Created on Apr 12, 2013

@author: alexandre
'''

"""
please, run this script using
mpirun -np 4 python asyncExample.py
"""

from jobDispatcher.mpi.async import AsyncFunc, dict_async, pool



def square(x): 
    return x**2

square_async = AsyncFunc(square)

pool.start()

    
d = dict_async()
for i in range(1,4):
    d[i] = square_async(i) 

print 'tmp dict:',d
pool.join() 
print 'final dict:',d
