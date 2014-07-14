#!/usr/bin/env python
'''
Dummy example to show how you can use MPI to compute the sum of elements.
'''
from __future__ import print_function
import sys
from pyjobber import dispatcher
from pyjobber.dispatch import newExperiment

# When importing your function, use the full module name from the actual PYTHONPATH
# if you use "from sum import parallelSum", it will work for this script, but you'll get an 
# ImportError when loading the function at execution of your job.
# The same problem happens if you define your function in the __main__ module 
from pyjobber.example.mpi4py.sum import parallelSum 


# In general, the dispatcher, need to know either the number of cores requested (nCpu) 
# or the number of nodes (nNode) with the number of processor per nodes (ppn) 
# since nCpu = ppn * nNode, only two of those 3 variables need to be specified.
# If you already specified the JD_PPN environment variable, you only have to specify nNode or nCpu.   
# nNode is often preferable since it is more portable from one cluster to another.
nNode = 1   


walltime = 60*10 # some clusters requires a walltime to accept job submission

# you can specify a different queue from the script parameters. For example 'debug' (if available on the current cluster)
if len(sys.argv) > 1: queue = sys.argv[1]
else:                 queue = None 

n = int(1e8) # summation will go from 0 to n

experiment = newExperiment('sum') # you always need to create an experiment first
print(experiment.folder)

job = experiment.newJob('sum') # an experiment usually have several jobs, in our case, we'll only have one 
job.setConf(nNode=nNode, mpi=True, walltime = walltime, queue=queue) # set the configuration file

# Create the callable object from function and argument list and write it on disk    
job.setFunction( parallelSum, end=n )

# obtain the dispatcher as specified by the environment variable 
# $DISPATCHER_TYPE = [ PbsDispatcher | SequentialDispatcher | SgeDispatcher ]
# some dispatcher requires a project id which can be specified by the variable $PROJECT_ID 
d = dispatcher.getHostDispatcher()  
d.submitExperiment(experiment)

