#!/usr/bin/env python

from __future__ import with_statement
import cPickle
from os import environ


# load the callable object
with open( 'callable.pkl', 'r' ) as fd:
    job = cPickle.load( fd )

# run the callable
out = job()

# save the output
if out is not None:
    if 'OMPI_COMM_WORLD_RANK' in environ:
        outName = 'out-%s.pkl'%environ['OMPI_COMM_WORLD_RANK']
    else:
        outName = 'out.pkl' 
        
    with open( outName, 'w' ) as fd:
        cPickle.dump( out, fd, 2 )

