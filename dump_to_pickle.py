import re
import json
import time
import threading
import _pickle as pickle
import gc
import sys
import os
import queue
import math
import pprint
import heapq

def writeToPickle(path):
    f = open(path, "r")
    offset = {}
    for x in f:
        print(x)
        dct = x.split(":")
        print(dct)
        break
        key = list(dct.keys())[0]
        offset[key] = dct[key] + 1
    f.close()

    data = postlist
    with open("offset_index", 'wb') as handle:
      pickle.dump(data, handle, protocol=-1)

writeToPickle("../wikidump_index/index/offset_index")
