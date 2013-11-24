#!/usr/bin/env python
'''
Simple script that submit jobs for profiling numpy.dot. This script should work on your local computer 
as well as any scheduler supported by jobDispatcher 
'''

from jobDispatcher import dispatcher
from jobDispatcher.dispatch import newExperiment


# When importing your function, use the full module name from the actual PYTHONPATH
# if you use "from approxPi import approxPi", it will work for this script, but you'll get an 
# ImportError when loading the function at execution of your job.
# The same problem happens if you define your function in the __main__ module 
from jobDispatcher.example.dotProfile.dotProfile import dotProfileList 


# In general, the dispatcher, need to know either the number of cores requested (nCpu) 
# or the number of nodes (nNode) with the number of processor per nodes (ppn) 
# since nCpu = ppn * nNode, only two of those 3 variables need to be specified.
# If you already specified the JD_PPN environment variable, you only have to specify nNode or nCpu.   
# nNode is often preferable since it is more portable from one cluster to another.

nNode = 1
walltime = 60*30 # some clusters requires a walltime to be between some minimum and maximum limits



experiment = newExperiment('dotProfile')
print experiment.folder

job = experiment.newJob( 'job' )
job.setConf(nNode = nNode, mpi=True, walltime = walltime) # set the configuration file

job.setFunction( dotProfileList, 100,100,100, 1e8)


# obtain the dispatcher as specified by the environment variable 
# $DISPATCHER_TYPE = [ PbsDispatcher | SequentialDispatcher | SgeDispatcher ]
# some dispatcher requires a project id which can be specified by the variable $PROJECT_ID 
d = dispatcher.getHostDispatcher()  


d.submitExperiment(experiment)

