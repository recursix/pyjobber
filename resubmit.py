#!/usr/bin/env python
'''
Created on Jan 31, 2011

@author: alex
'''


import sys
from os import path
from jobDispatcher import dispatch
from jobDispatcher.dispatcher  import getHostDispatcher
from util import formatTimeStr, day
import os

# Note: to use resubmit.py:
# resubmit.py --nCpu N --walltime days:hours:minutes:seconds ABSOLUTE_JOBFOLDER


expsFolder = os.environ['EXPERIMENTS_FOLDER']

argL = sys.argv[1:]

expName = argL.pop() # last arg is the folder


folder = path.join(expsFolder, expName )

nCpu = None
walltime = None
nNode = None

for i,arg in enumerate(argL):
    if arg == '--nCpu':
        nCpu = int(argL[i+1])
    if arg == '--walltime':
        walltime = formatTimeStr(argL[i+1])
    if arg == '--nNode':
        nNode = int(argL[i+1])

if nCpu is not None:
    assert nCpu < 10000, "don't you think you are exaggerating ..."
    assert nCpu > 0, "dont't try to fool me..."
    print "will reconfigure nCpu to %d"%nCpu

if nNode is not None:
    assert nNode < 10000, "don't you think you are exaggerating ..."
    assert nNode > 0, "dont't try to fool me..."
    print "will reconfigure nNode to %d"%nNode


    
if walltime is not None:
    assert walltime > 0, "Negative walltime is impossible."
    assert walltime < 365*day, "excessive walltime."
    print "will reconfigure walltime to %.3fs"%walltime

    
if not path.exists(folder):
    print "Job root %s doesn't exist"%folder
    sys.exit(1)

experiment = dispatch.Experiment(folder)

dispatcher = getHostDispatcher()

nameL = []
for job in experiment:
    if dispatch.basicFilter(job):
        nameL.append( job.name )
        
if len(nameL) > 0 :
    print "%d jobs need to be resubmitted:"%(len(nameL))
    print '* ' + '\n* '.join(nameL)
else:
    print "nothing needs to be resubmitted."
    sys.exit(0)
print




# by default does not resubmit jobs that have finished with no error

print "Dispatcher type : %s"%(str(dispatcher))
while True:
    cmd = raw_input( 'would you like to resubmit? y,n : ' )
    
    if cmd == 'n':
        print 'not resubmitting'
        break
    elif cmd == 'y':
        
        print "reconfiguring jobConf (if needed)"
        for job in experiment:
            if dispatch.basicFilter(job):
                
                job.setConf(
                    nCpu = nCpu,
                    nNode = nNode,
                    walltime = walltime,
                    )
                
        
        print 'resubmitting'
        dispatcher.submitExperiment(experiment)
        break
    else:
        print 'please answer y or n'

    

     
