'''
Created on Jul 16, 2010

@author: alex
'''


import unittest
#from pyon.mpiPool3 import Worker, TaskHolder, Pool, MpiQueue, myRank
from pyon import mpiPool4 as mp


import time as t

def add( x, y ):
    return x+y



class Result(dict):
        
    def set(self, out, id ):
        self[id] = out



class TestQueue(unittest.TestCase):
    
    def setUp(self):
        self.queue = mp.MpiQueue()
        
    def testPutGet(self ):
        taskId = 10
        self.queue.putTask(mp.myRank, taskId, 0)
        (rank_, taskId_) = self.queue.getTask()
        self.assertEqual( rank_, mp.myRank )
        self.assertEqual( taskId_, taskId )
        
    def testCancel( self):
        taskId = 11
        self.queue.putTask( mp.myRank, taskId, 0 )
        self.queue.delTask( mp.myRank, taskId )
        
        (_rank ,taskId_) = self.queue.getTask()
        self.assertEqual( taskId_, mp.EMPTY_QUEUE )
        
    def testPriority( self ):
        pL = [ 1, 3, 2 ]
        
        taskD = {}
        idCount = 20
        
        for p in pL:
            self.queue.putTask(mp.myRank, idCount, p)
            taskD[idCount] = p 
            idCount += 1
            
        # change the priority
        for id, p in taskD.iteritems():
            
            if p == 2:
                newP = 10
                taskD[id] = newP
                self.queue.modifyTask( mp.myRank, id, newP )
            
            if p == 1 : 
                newP = 4
                taskD[id] = newP
                self.queue.modifyTask( mp.myRank, id, newP )
                
        for p in sorted( taskD.values(), reverse=True):
            (_rank,taskId_) = self.queue.getTask()
            self.assertEqual( taskD[taskId_], p )
            
        
#class TestWorker(unittest.TestCase):
#    
#    def setUp(self):
#        self.worker = mp.Worker()
#        self.worker.start()
#        
#    def testExec(self):
#        x = 2
#        y = 3
#        z = add(x,y)
#        self.worker.task = (add, (x,y), {})
#        t.sleep(mp.tinyDelay*10)
#        self.assertEqual(z, self.worker.out )
#        
#        
#    def tearDown(self):
#        self.worker.stop()
#        self.worker.join(mp.tinyDelay*10)
#        self.assertFalse( self.worker.is_alive() )
    


class TestPool(unittest.TestCase):
    
    def setUp(self):
        
        self.pool = mp.pool
        self.pool.start()
        self.result = Result()
    
    
    def testExecute(self):
        x = 3
        y = 4
        z = x+y
        resId = 1
        
        taskRef = self.pool.apply_async( add, (x,y), {}, self.result.set, (resId,) )
#        while not  self.result.has_key(resId):
#            t.sleep(mp.tinyDelay)

        t.sleep( mp.tinyDelay* 10 )
        self.assertEqual( self.result[resId], z )
    
    def tearDown(self):
        self.pool.stop()

if __name__ == '__main__':
    unittest.main()