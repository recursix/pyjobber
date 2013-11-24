#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2012-07-08

@author: alexandre

this scripts links one process with another
In case the master process dies, it will also send a signal to the linked process
This is usefull since pyDev kill (instead of terminate) your python program when you click 
on the red square. Then, any subprocess will continue running. Using this script, you can
solve this problem. 
'''
from __future__ import with_statement
import sys
import time as t
import os
import signal

child = int(sys.argv[1])
if len(sys.argv) > 2:  delay = float(sys.argv[2])
else:                  delay = 0.1
if len(sys.argv) > 3:  sig = int(sys.argv[3])
else:                  sig = signal.SIGTERM

while True:
    t.sleep(delay)
    if os.getppid() == 1:
        try:
            with open( '/tmp/linkChild', 'a' ) as fd:
                fd.write( 'killing pid %d\n'%child )
            os.kill(child, sig)
        except OSError: pass
        sys.exit()
        
        