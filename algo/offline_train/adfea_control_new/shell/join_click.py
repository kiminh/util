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





#09335454-A7B2-4FE4-922C-DBE4DEF41774^Icom.tinmanarts.MagicCake:2 com.tinmanarts.MagicIcecream:2
def process(inclick,inshitu,outshitu):

    click_dict = {}
    for raw_line in open(inclick):
        line = raw_line.lower().rstrip("\r\n").strip().split("\t")
        auction = "%s_%s"%(line[2],line[3])
        click_dict[auction] = line[1]

    ##inshitu
    
    fp_w = open(outshitu,"w")
    for raw_line in open(inshitu):
        line = raw_line.lower().rstrip("\r\n").strip().split("\001")
        auction = "%s_%s"%(line[4],line[5])
        label = int(line[0])
        if label == 1:
            fp_w.write(raw_line)
            continue
        if auction in click_dict:
            print "hit late click[%s]!"%(click_dict[auction])
            label = 1
        fp_w.write("%d\001%s\n"%(label,"\001".join(raw_line.rstrip("\r\n").split("\001")[1:])))

    fp_w.close()
            

            

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "Usage %s click_file shitu_log_ori shitu_log_final"%(sys.argv[0])
        sys.exit(1)
    process(sys.argv[1],sys.argv[2],sys.argv[3])
    
