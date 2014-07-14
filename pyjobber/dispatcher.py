'''
Created on May 30, 2010

@author: alex
'''

from __future__ import print_function
from pyjobber import dispatch as d
import subprocess as sp
from os import path, environ


class PbsDispatcher(d.Dispatcher):
    
    
    def __str__(self):
        return "PbsDispatcher"
    
    
    def submit( self, job ):
        projectId = getattr(self,'projectId',None)

        scriptPath = job._writeSubmitScript()
        pbsQsub( scriptPath, job.conf.queue, job.folder, 
            job.conf.name, job.conf.walltime , job.conf.nNode, job.conf.ppn,
            envVarL=['RUN_CALLABLE'],
            stdout='submit.stdout',
            stderr='submit.stderr', projectId=projectId  )

class MoabDispatcher(d.Dispatcher):
    
#    def __init__(self, projectId = None,verbose=1 ):
#        d.Dispatcher.__init__(self,verbose) 
#        self.projectId = projectId
    
    def __str__(self):
        return "MoabDispatcher"
    
    
    def submit( self, job ):
        projectId = getattr(self,'projectId',None)
        
        scriptPath = job._writeSubmitScript()
        pbsQsub( scriptPath, job.conf.queue, job.folder, 
            job.conf.name, job.conf.walltime , job.conf.nNode, job.conf.ppn,
            envVarL=['RUN_CALLABLE'],
            stdout='submit.stdout',
            stderr='submit.stderr', projectId=projectId, cmdName='msub'  )

    
class SgeDispatcher(d.Dispatcher):
    
    def __init__(self, projectId = None,verbose=1 ):
        d.Dispatcher.__init__(self,verbose) 
        self.projectId = projectId
        
    def __str__(self):
        return "SgeDispatcher( projectId=%s )"%( self.projectId )
    
    def submit( self, job ):
        scriptPath = job._writeSubmitScript()
        sgeQsub(scriptPath, [], job.conf.name, self.projectId, job.conf.queue, job.folder,
            job.conf.nCpu, job.conf.walltime, job.conf.priority, 
            envVarL=['RUN_CALLABLE'],
            stdout=path.join(job.folder,'submit.stdout'),
            stderr=path.join(job.folder,'submit.stderr') ) 

   
        
class SequentialDispatcher(d.Dispatcher):
    def __str__(self):
        return "SequentialDispatcher"
    
    def submit( self, job ):
        if job.conf.ppn is None:
            import multiprocessing as mp
            nCpu = mp.cpu_count()
            if self.verbose > 0 : print("nCpu is unspecified. Using %d cpu."%nCpu)
            job.setConf( ppn=nCpu)
        if self.verbose > 0 : print('%s running %s'%( str(self), str(job.conf)))
        job.run()

    
def pbsQsub( cmd, queue=None, wd=None, name=None, 
    walltime=None, nNode=None, ppn=None,stdout=None, stderr=None,envVarL=[], projectId=None,cmdName='qsub'):
    
    cmdL = [cmdName]
    
    if name is not None:  cmdL += ['-N', name ]
    if projectId is not None: cmdL += [ '-A', projectId ]
    if queue is not None: cmdL += ['-q', queue ]
    if wd is not None:    cmdL += ['-d', wd ]
    if len(envVarL) > 0:  cmdL += ['-v', ','.join(envVarL) ]
    
    if stdout is not None: cmdL += ['-o', stdout ]
    if stderr is not None: cmdL += ['-e', stderr ]
    
    ressourceL = []

    if nNode is None : nNode = 1

    if ppn is None: ressourceL.append( 'nodes=%d'%(nNode) )
    else :          ressourceL.append( 'nodes=%d:ppn=%d'%(nNode,ppn) )
    if walltime is not None: ressourceL.append('walltime=%.f'%walltime )
    if len(ressourceL) > 0: cmdL += ['-l', ','.join(ressourceL) ]
    

    cmdL.append( cmd )
    print(' '.join( cmdL ))
    sp.call(cmdL)
    
    


def sgeQsub( cmd, argL=[], jobName=None, projectName=None, queue=None, wd=None, 
    nCpu=None, walltime=None, priority=None, 
    stdout=None, stderr=None, envVarL=[], opt=[]):
    
    cmdL = ['qsub'] + opt
    
    if projectName is not None:   cmdL += ['-P', projectName]
    if queue is not None:         cmdL += ['-q', queue ]
    if wd is not None:            cmdL += ['-wd', wd]
    if jobName is not None:       cmdL += ['-N', jobName]
    if len(envVarL) > 0:          cmdL += ['-v', ','.join(envVarL)]
    if priority is not None:      cmdL += ['-p', float(priority) ]
    
    if nCpu is not None and nCpu > 1:
        cmdL += [ '-pe', 'default', '%d'%(nCpu) ]

    ressourceL = []
    if walltime is not None: ressourceL.append('h_rt=%.f'%walltime )
    if len(ressourceL) > 0: cmdL += ['-l', ','.join(ressourceL) ]
    
    if stdout is not None: cmdL += ['-o', stdout ]
    if stderr is not None: cmdL += ['-e', stderr ]
    
    cmdL.append( cmd )
    if len(argL) > 0: cmdL += [ '--' ] + argL 
    print(' '.join( cmdL ))
    sp.call(cmdL)


class UndefinedEnvironmentVariable(Exception): pass
class UnknownDispatcherType(Exception): pass

dispatcherMap = {
    'PbsDispatcher' : PbsDispatcher,
    'SequentialDispatcher' : SequentialDispatcher,
    'SgeDispatcher' : SgeDispatcher,
    'MoabDispatcher': MoabDispatcher,
    }
    

def getHostDispatcher():
    if not 'DISPATCHER_TYPE' in environ: 
        print("WARNING : environment variable $DISPATCHER_TYPE is not defined, using SequentialDispatcher")
        dispatcherType= "SequentialDispatcher"
    else : 
        dispatcherType = environ['DISPATCHER_TYPE']
    
    if dispatcherType not in dispatcherMap:
        raise UnknownDispatcherType('Invalid dispatcher! Please, use one of {%s}'%(', '.join( dispatcherMap.keys() )) )
    
#    argD = {}
#    if 'DISPATCHER_QUEUE' in environ:
#        argD['queue'] = environ['DISPATCHER_QUEUE']
#    if 'PROJECT_ID' in environ:
#        argD['projectId'] = environ['PROJECT_ID']


    dispatcher =  dispatcherMap[dispatcherType]()
    dispatcher.projectId = environ.get("PROJECT_ID",None)
    
    return dispatcher

    
    
