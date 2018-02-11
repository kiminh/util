# Copyright (c) MetaCommunications, Inc. 2003-2005
#
# Distributed under the Boost Software License, Version 1.0. 
# (See accompanying file LICENSE_1_0.txt or copy at 
# http://www.boost.org/LICENSE_1_0.txt)
#--coding:utf-8-- 

import os
import re
import string
import sys
import json





def process(inbatch,incre,outfile):

    incre_dict = {}
    for raw_line in open(incre):
        line = raw_line.rstrip("\r\n").strip().split(" ")
        key = line[0]
        train = " ".join(line[1:])
        incre_dict[key] = train
    
    fp_w = open(outfile,"w")
    for raw_line in open(inbatch):
        line = raw_line.lower().rstrip("\r\n").strip().split(" ")
        key = line[0]
        train = " ".join(line[1:])
        if key not in incre_dict:
            print "key[%s] miss in batch"%(key)
            continue
        new_train = incre_dict[key]
        fp_w.write("%s %s %s\n"%(key,train,new_train))

    fp_w.close()
            

            

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "Usage %s batch_fea incre_fea output"%(sys.argv[0])
        sys.exit(1)
    process(sys.argv[1],sys.argv[2],sys.argv[3])
    
