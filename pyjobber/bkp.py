# -*- coding: utf-8 -*-
'''
Created on Jun 21, 2013

@author: alexandre
'''

    
from __future__ import print_function
from threading import Thread
import time as t
from graalUtil import file
import os
from mpi.mpiPool import pool
from os import path







class BackedUpMpiPoolJob:

    """
    async_result = func( *arg_list, **arg_dict) will be executed in the mpiPool environment and the 
    asynchronous result will be backed up every bkp_delay seconds.  
    """
    
    def __init__(self,  bkp_delay=60, func=None, *arg_list, **arg_dict ):
        self.bkp_delay = bkp_delay
        self.func = func
        
        self.arg_list = arg_list
        self.arg_dict = arg_dict

    def __call__(self):
        pool.start() # only main mpiRank can enter this loop
            
        async_result = self.func( *self.arg_list, **self.arg_dict )
        bkp = Bkp( async_result, delay=self.bkp_delay )  # this will periodically backup the result
        print 'backup started'
        pool.stop()
        bkp.backup() # one last backup
        bkp.stop()



class Bkp(Thread):
    
    def __init__(self,obj,bkp_folder='.',delay=60):
        Thread.__init__(self)
        
        self.obj = obj
        self.bkp_folder = bkp_folder
        self.delay = delay 
        self.lastBackup = 0
        self.done = False
        self.start()
            
    def run(self):
        while not self.done:
            if t.time() > self.lastBackup + self.delay:
                self.backup()
                
#            dt =  self.delay + self.lastBackup - t.time()
#            dt = max(dt,0.1)
            t.sleep( 0.1 )
            
        self.backup()

        
    def backup(self):
        # Writing backup in two steps. This avoids the case where
        # the processes is being killed while the bkp is partly overwritten.
        
#        print 'writing bkp %s'%self.bkpPath
        try:
            if hasattr(self.obj, "extract"):
                bkp_dict = self.obj.extract()
            else:
                bkp_dict = {'out': self.obj}
            
            for name, obj in bkp_dict.items():
                tmp_bkp_path = path.join(self.bkp_folder, '%s-tmp.pklz'%name )
                bkp_path = path.join(self.bkp_folder, '%s.pklz'%name )
                file.writePklz( obj, tmp_bkp_path )            
                os.rename(tmp_bkp_path, bkp_path) 
                
            self.lastBackup = t.time()
        except RuntimeError:
            if self.done: raise
            else:
                print 'Error occurred during backup. Will try again...'
        
            
    def stop(self):
        self.done = True
        self.join()
#        if path.exists( self.bkpPathTmp ):
#            os.remove(self.bkpPathTmp)
            
            
