#!/usr/bin/env python

from __future__ import with_statement
from os import environ

# Python 2 and 3 support
try:
    import cPickle as pickle
except:
    import pickle



# load the callable object
with open( 'callable.pkl', 'rb' ) as fd:
    job = pickle.load( fd )

# run the callable
out = job()

# save the output
if out is not None:
    if 'OMPI_COMM_WORLD_RANK' in environ:
        outName = 'out-%s.pkl'%environ['OMPI_COMM_WORLD_RANK']
    else:
        outName = 'out.pkl' 
        
    with open( outName, 'wb' ) as fd:
        pickle.dump( out, fd, 2 )

