'''
Created on Aug 17, 2010

@author: alex
'''
import fileUtil
from pyon.defer import Deferred


bkp = fileUtil.readPklz('bkp/exBkp5.pklz')
for val in bkp.argDD.itervalues():
    
    if isinstance(val,Deferred): 
        print val.waiting
    else:
        print val


bkp.wake()
bkp.wait()

print bkp.answer