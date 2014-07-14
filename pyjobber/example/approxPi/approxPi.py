# -*- coding: utf-8 -*-
'''
Created on 2012-06-21

@author: alexandre
'''

from random import random

# Python 2 and 3 support
try:
    range = xrange
except NameError:
    pass


def approxPi(nSamples):
    count = 0
    for _i in range(nSamples):
        x = random()
        y = random()
        if x**2 + y**2 < 1:
            count += 1
            
    return 4*count/ float(nSamples)


if __name__ == "__main__":
    print(approxPi(1000000))
    
