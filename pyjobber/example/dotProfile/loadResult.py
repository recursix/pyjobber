# -*- coding: utf-8 -*-
'''
Created on 2012-07-04

@author: alexandre
'''
from jobDispatcher.dispatch import getLastExperiment
from dotProfile import plotResult

experiment = getLastExperiment()
print experiment.folder

job = list(experiment)[0]
#for job in experiment:
resD = job.getResultList()[0]


print job.getStdout()
print job.getStderr()

plotResult(resD)
