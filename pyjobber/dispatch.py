from __future__ import with_statement, print_function
from os import path
import os
from pyjobber.util import readFile, writeFile, readPkl, writePkl, replaceD, readFileGz, Callable
import subprocess as sp
import time as t
import re
import sys

# Support both python 2 and 3
try:
    range = xrange
except NameError:
    pass

# gather some paths
jobDispatcherFolder = path.dirname( path.abspath( __file__ ) )
runCallablePath = path.join( jobDispatcherFolder , 'runCallable.py' )
launcherPath = path.join( jobDispatcherFolder , 'launcher.py' )
os.environ["RUN_CALLABLE"] = runCallablePath
linkChildPath = path.join( jobDispatcherFolder, 'linkChild.py' )

if not 'EXPERIMENTS_FOLDER' in os.environ:
    experimentsFolder = path.expandvars( path.join( "$HOME", 'experimentsFolder' ) )
    print("WARNING : environment variable $EXPERIMENTS_FOLDER not defined, using %s"%experimentsFolder)
else:
    experimentsFolder = os.environ['EXPERIMENTS_FOLDER']


if not path.exists(experimentsFolder):
    os.mkdir(experimentsFolder)


class JobConf: 


    def __init__(self, jobName):
        self.name = replaceD( jobName, {'/':'-',':':'.',',':'_','*':'_','(':'_',')':'_','>':'_','=':'_','[':'_',']':'_' } )
        self.mpi = False
        self.splitStdout = True
        self.nCpu = None
        self.nNode = None
        self.ppn = None
        self.queue = None
        self.walltime = None
        self.priority = None
        self.compressStdout = True
        
        
    def __str__(self):
        strL = []
        if self.mpi : 
            strL.append( 'mpi' )
        if self.nCpu is not None:
            strL.append( 'nCpu : %d'%self.nCpu)
        if self.nNode is not None:
            strL.append( 'nNode : %d'%self.nNode)
        if self.ppn is not None:
            strL.append( 'ppn : %d'%self.ppn)
        if self.walltime is not None:
            strL.append( 'walltime : %s'%(str(self.walltime)) )
        if self.priority is not None:
            strL.append( 'priority : %s'%(str(self.priority)))
        if self.queue is not None:
            strL.append( 'queue : %s'%(self.queue))
        
        return 'job %s { %s }'%(self.name, ', '.join(strL))



class Std:
    
    mpiPattern = re.compile("(\[[0-9,]+\]<std[a-z]+>:)(.*)") 
    
    def __init__(self, folder, filter='stdout'):
        self.filter = filter
        self.folder = folder
        self.stdout = {}
        self._load()
        
    def _load(self):
        for name in os.listdir(self.folder):
            if name.find(self.filter) >= 0:
                        
                if name.endswith('.gz'):
                    content = readFileGz(self.folder, name)
                else:
                    content = readFile(self.folder, name)
                    
                if not self._parseMpiStdoutFile(content,name):
                    self.stdout[name]= content
                
    
    def _parseMpiStdoutFile(self, content, name):
        
        if not content.strip().startswith('['): return False
        
        lineLD = {}
        for line in content.splitlines():
            match = self.mpiPattern.match(line)
            if match is None: 
#                print('not matching line %s'%line)
                return False
            
            key = match.group(1)
            if not key in lineLD: lineLD[key] =[] 
            lineLD[key].append( match.group(2) )
            
        for key, lineL in lineLD.items():
            self.stdout[ name + '/' + key ] = '\n'.join(lineL)
        return True
    
    def __str__(self):
        strL = []
        keyL = list(self.stdout.keys())
        keyL.sort()
        for key in keyL:
            if len(self.stdout[key].strip()) > 0: 
                strL.append(key)
                strL.append('-'*len(key))
                strL.append(self.stdout[key])
                strL.append('')
    
        return '\n'.join(strL)
        
class Job:

    
    def __init__(self, folder ):
        self.name = path.basename(folder)
        self.folder = folder
        if not path.exists( self.folder ): os.makedirs( self.folder )
        self._loadConf()

    def getStdout(self):  return Std( self.folder, 'stdout' )
    def getStderr(self):  return Std( self.folder, 'stderr' )
    
    def getCallable(self): return readPkl(  self.folder, 'callable.pkl' )
    def getResult(self):
        res = readPkl(  self.folder, 'out.pkl', defaultVal=None )
        if res is not None : return res
        return readPkl(  self.folder, 'out-0.pkl', defaultVal=None ) # return the master rank
    
    def getResultList(self):
        result = readPkl( self.folder, 'out.pkl', defaultVal=None )
        if result is not None:
            return [result] 
        
        resD = {}
        for name in os.listdir(self.folder):
            if name.startswith('out') and name.endswith('.pkl'):
                idx = int( re.search('\d+', name ).group(0) )
                resD[idx] =  readPkl( self.folder, name ) 
        
        if len(resD) == 0 : return []
        
        resL = [None]*( max(resD.keys()) + 1 )
        for idx, res in resD.items():
            resL[idx] = res
            
        return resL
    

    
    def setConf(self, nCpu=None, ppn=None, nNode=None, mpi=None, walltime=None, priority=None, 
            queue=None, splitStdout=None, compressStdout=None ):
        
        if nCpu           is not None : self.conf.nCpu = nCpu
        if nNode          is not None : self.conf.nNode = nNode
        if ppn            is not None : self.conf.ppn = ppn
        if mpi            is not None : self.conf.mpi = mpi
        if walltime       is not None : self.conf.walltime = walltime
        if priority       is not None : self.conf.priority = priority
        if queue          is not None : self.conf.queue = queue
        if splitStdout    is not None : self.conf.splitStdout = splitStdout
        if compressStdout is not None : self.conf.compressStdout = compressStdout
        
        # complete the trio
        self.conf.nCpu, self.conf.nNode, self.conf.ppn = checkNcpu(
            self.conf.nCpu, self.conf.nNode, self.conf.ppn )
        
        self._saveConf()
    
    def _saveConf(self):
        writePkl( self.conf, self.confPath() )
        
    def confPath(self):
        return path.join( self.folder, 'conf.pkl' )
    
    def _loadConf(self):
        confPath = self.confPath()
        if not path.exists(confPath ):
            self.conf = JobConf( self.name )
        else:
            self.conf = readPkl( confPath ) 
        

        
        
    def writeCmd(self, cmd ):
        cmdPath = path.join( self.folder, 'run' )
        writeFile( cmd, cmdPath  )
        os.chmod( cmdPath, 0o755 )

    def _writeSubmitScript(self):
        """mostly to be able to source .qsubrc on linux or qsubrc.bat on windows"""
        
        if sys.platform.startswith('win'):
            scriptPath = path.join(self.folder, 'submit.bat' )
            qsubRc = path.expandvars( path.join("$HOME","qsubrc.bat") )
            cmdL = ["@ECHO off"]
            
            if path.exists(qsubRc):
                cmdL.append('call "%s"'%qsubRc) # didn't actually tried if this feature works :p
                
            cmdL.append('"%s" "%s"'%( sys.executable, launcherPath ) )
        else:
            scriptPath = path.join( self.folder, 'submit.sh' )
            qsubRc = path.expandvars( path.join("$HOME",".qsubrc") )
            cmdL = ["#!/usr/bin/env bash"]

            if path.exists(qsubRc):
                cmdL.append('source "%s"'%qsubRc)
                    
            cmdL.append('"%s" "%s"'%( sys.executable, launcherPath ) )
        writeFile( '\n'.join( cmdL ), scriptPath )
        os.chmod(scriptPath, 0o755)
        return scriptPath
        
    
        
    
#    def writeCallable( self, callable_  ):
#        writePkl(callable_, self.folder, 'callable.pkl' )

    def setFunction(self, f, *argL, **argD ):
        callable_ = Callable( f, argL, argD ) 
        writePkl(callable_, self.folder, 'callable.pkl' )


    def haveErrors(self):
        stderr = str(self.getStderr())
        if   stderr is None:                return False
        elif len( stderr.strip() ) == 0:    return False
        else:                               return True

    def run(self):
        scriptPath = self._writeSubmitScript()
        stdout, _stderr = call_err( [scriptPath], cwd=self.folder,linkChild=True )
        if len(stdout.strip()) > 0: # should be empty but can be useful for debugging purpose
            print(stdout)
            
    def getState(self):
        stateStr = readFile( self.folder, 'jobState' )
        if stateStr is not None:
            return stateStr.splitlines()
        else: return None


    def isDone(self):
        state = self.getState()
        if state is None: return False
        return ( state[-1].lower().startswith( 'done' ) )


def checkNcpu( nCpu=None, nNode=None, ppn=None):

    # try to infer the 3rd value from the two others
    if nCpu is None and ppn is not None and nNode is not None:
        nCpu = nNode * ppn
    if ppn is None and nCpu is not None and nNode is not None:
        if nCpu % nNode == 0: ppn = nCpu // nNode
    if nNode is None and nCpu is not None and ppn is not None:
        if nCpu % ppn   == 0: nNode = nCpu // ppn
    
    return nCpu, nNode, ppn

def loadPpnEnvVar(  ):
    try : return int( os.environ['JD_PPN'] ) 
    except ValueError : pass # not an integer
    except KeyError   : pass # not defined

def newExperiment( name, addTimeStamp=True, expsFolder=None ):
    if expsFolder is None : 
        expsFolder = experimentsFolder
    
    if addTimeStamp :
        name  = name + t.strftime('_%d%b%Y-%H.%M.%S')
        
    return Experiment( path.join( expsFolder, name ) )

class Experiment:
    
    def __init__(self, folder ):
        
        self.folder = folder
        
        if not path.exists( self.folder ):
            os.makedirs(self.folder)
        

    def getJobList( self ):
        """
        This function recursively extracts the list of jobs from the root.
        It searches for folders containing a file named 'run' and not containent a file name 'wrapped'.
        It wont search through hidden folders (starting with .).
        """
        
        jobList = []
        for cwd, dirs, files in os.walk(self.folder):
            
            if ('run' in files) or ('callable.pkl' in files):
                jobList.append( Job(cwd) )
                
            # don't walk through hidden dirs
            for i in reversed( range( len(dirs ) ) ):
                if dirs[i].startswith('.'): del dirs[i]
                
        return jobList
    
    def __iter__(self):
        jobL = self.getJobList()
        return jobL.__iter__()
    
    def newJob( self, jobName=None ):
        if jobName is None:
            return Job( self.folder )
        else:
            return Job( path.join( self.folder, jobName ) )
        
        

def basicFilter(job):
    """filter completed jobs"""
    if job.isDone() and not job.haveErrors():
        return False
    return True
    

class Dispatcher:
    
    def __init__(self,verbose=1):
        self.verbose = verbose
    
    """Basic interface for dispatcher"""
    def submitExperiment( self, experiment, fltr=basicFilter, waiter=None ):
        for job in experiment:
            if fltr(job):
                
#                if waiter is not None:  waiter.wait()
                
                # at submit time, you may have information about ppn with environment variable
                if job.conf.ppn is None:
                    ppn = loadPpnEnvVar()
                    if ppn is not None: job.setConf( ppn = ppn )
                
                if self.verbose >0:
                    print('%s submitting %s'%(str(self),str(job.conf)))
                self.submit(job)
            else:
                print('skipping : %s'%job.folder)

    
def call_err(*popenargs, **kwargs):
    linkChild = False
    if "linkChild" in kwargs:
        linkChild = kwargs.pop('linkChild')
    
    process = sp.Popen(stdout=sp.PIPE, stderr=sp.PIPE, *popenargs, **kwargs)
    
    if linkChild: # this will ensure that pyDev SIGKILL (red square) will also kill the subprocess
        sp.Popen([sys.executable, linkChildPath, str(process.pid), '0.1' ] )
        
    out, err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise CalledProcessError(retcode, cmd, out=out, err=err)
    return out,err
#

class CalledProcessError(Exception):
    """This exception is raised when a process run by check_call() or
    check_output() returns a non-zero exit status.
    The exit status will be stored in the returncode attribute;
    check_output() will also store the output in the output attribute.
    """
    def __init__(self, returncode, cmd, out=None, err=None):
        self.returncode = returncode
        self.cmd = cmd
        self.out = out
        self.err = err
        
    def __str__(self):
        err = indent(self.err)
        out = indent(self.out)
        return "Command '%s' returned non-zero exit status %d. \nSTDOUT : \n%s\nSTDERR : \n%s" % (self.cmd, self.returncode, out,err)

def indent(txt, prefix='    '):
    return '\n'.join( [ prefix+line for line in txt.split('\n') ] )
    

def getLastExperiment(prefix='', expsFolder=None):
    if expsFolder is None:
        expsFolder = experimentsFolder
        
    cTime = 0.
    lastExpFolder = None
    for name in os.listdir(expsFolder):
        expFolder = path.join(expsFolder,name)
        
        if not path.isdir( expFolder ) : continue # ignore non directory
        if not name.startswith(prefix) : continue # ignore 
        
        t= path.getctime(expFolder)
        if t > cTime:
            cTime = t
            lastExpFolder = expFolder
    
    if lastExpFolder is None: return None
    return Experiment(lastExpFolder)
