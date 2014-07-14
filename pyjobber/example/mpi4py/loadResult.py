from __future__ import print_function
from pyjobber.dispatch import getLastExperiment

experiment = getLastExperiment('sum')
print("Experiment folder : ", experiment.folder)


job = experiment.getJobList()[0]

# If you use MPI and all rank return a result different than None, than you should use Job.getResultList()
# if you are not using MPI or only rank-0 returns a result, use Job.getResult() 
resL = job.getResultList() 


s = sum(resL) # sum the partial results
print("sum is %d"%s)

# for debugging
print(job.getStdout())
print(job.getStderr())

# to verify the answer
n = job.getCallable().argD['end']
assert (n*(n-1))/2 == s  
