# -*- coding: utf-8 -*-
'''
Created on 2012-07-04

@author: alexandre
'''
from jobDispatcher.dispatch import getLastExperiment

experiment = getLastExperiment()
print experiment.folder

piL = []
for job in experiment:
    resL = job.getResultList() 
    
    print job.name, ' : ', resL
    piL += resL

pi = sum(piL)/len(piL)
print "pi is approximately %.6f"%(pi)