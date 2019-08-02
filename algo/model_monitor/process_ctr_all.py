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
def process(infile,outfile1,outfile2):
    ##app
    ##total cnt
    fp_w = open(outfile1,"w")
    fp_w2 = open(outfile2,"w")
    line_cnt = 0
    for raw_line in open(infile):
        line_cnt +=1
        line = raw_line.lower().rstrip("\r\n").strip().split("\001")
#'{print $34}'
        try:
            info = json.loads(line[37])
            pctr = float(info["pctr"])
        except:
            print "format[%s] error[%d]"%(raw_line,line_cnt)
            continue

 #       prices = line[33][1:len(line[33])-1].split(",")
  #      price = float(prices[0])
   #     priority = float(prices[1])

        #"ctr_id":2
       # ctr_id = int(info["ctr_id"])
        #if ctr_id == 1 or ctr_id == 3:
         #   fp_w.write("%s\t%f\n"%(raw_line.lower().rstrip("\r\n"),pctr))
        #fp_w.write("%f\t%f\t%f\t%s"%(pctr,price,priority,raw_line.lower()))
        fp_w.write("%f\n"%(pctr))
        fp_w2.write(raw_line)


    fp_w.close()
    fp_w2.close()
            

            

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "Usage %s infile outfile1 outfile2"%(sys.argv[0])
        sys.exit(1)
    process(sys.argv[1],sys.argv[2],sys.argv[3])
