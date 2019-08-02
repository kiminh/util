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



{"owner":"1","crid":"100000706","dpid":"6ea56e5d-f526-4229-870a-1a81437d9337","deviceString":"352112074345528","auction":"81c497b4d50011e6992000163e00134d","aduser":"100088","adid":"1000279","vendor":"70013","appid":"100069","supplier":"1","adspot":"10000148","action":"click","account_list":["1000279"],"company":"18","strategy":"1","_time":"2017-01-08 02:00:01,221","impid":"c175957884f43e31bb7f80e6d5629417","account":"1000279"}



def process(click_file,action_file,conversion_file):


#ACTIVE  2017-Jan-13 14:08:51.89293      6c129070cd7e711e6992000163e00134d       10000148        1000279
    
    action_set = set()
    for raw_line in open(action_file):
        line = raw_line.rstrip("\n\r").split("\t")
        if len(line) == 1:
            info_dict = json.loads(raw_line)
            auction_id = info_dict["auction"]
            adspot = info_dict["adspot"]
            print "format is old"
        else:
            auction_id = line[2]
            adspot = line[3]
            print "format is  new"
        key = "%s_%s"%(auction_id,adspot)
        action_set.add(key)

    fp_w = open(conversion_file,"w")
    for raw_line in open(click_file):
        line = raw_line.rstrip("\n\r").split("\001")
        auction_id = line[4]
        adspot = line[5]
        key = "%s_%s"%(auction_id,adspot)
        label = 0
        if key in action_set:
            label = 1
        fp_w.write("%d\001%s\n"%(label,raw_line.rstrip("\n\r")))

    fp_w.close()

            

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "Usage %s batch_click action batch_conversion"%(sys.argv[0])
        sys.exit(1)
    process(sys.argv[1],sys.argv[2],sys.argv[3])
    
