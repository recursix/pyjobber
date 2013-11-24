# -*- coding: utf-8 -*-
'''
Created on Nov 24, 2013

@author: alexandre
'''

from jobDispatcher.mpi import mpiPool
pool = mpiPool.Pool()

import numpy as np



res_list = [ pool.apply_async(np.sin, (x,)) for x in range(10000) ]

i = 0
for res in res_list:
    x = res.get()
    assert x == np.sin(i)
    i += 1
    
    
print 'done %d'%i    