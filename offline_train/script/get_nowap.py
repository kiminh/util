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
def process(infile,nowapfile):
    ##app
    ##total cnt
    fp2_w = open(nowapfile,"w")

    line_cnt = 0
    for raw_line in open(infile):
        line_cnt +=1
        line = raw_line.lower().rstrip("\r\n").strip().split("\001")
        if len(line) < 38:
            continue
        try:
            meta_info = json.loads(line[37])
        except:
            print raw_line

        native_type = 0
        wap = 0
        image_list = []
        word_list = []
        freq_cid = 0
        freq_adid = 0
        creative_id = line[38]

        try:
            augments = meta_info["augments"]       
            freq_info = augments[creative_id]
            freq_cid = freq_info["freq_cid"]
            freq_adid = freq_info["freq_adid"]
        except:
            pass
        try:
            ext = meta_info["ext"]
            wap = meta_info["wap"]
            native_type = json.loads(ext)["native_adtype"]
            native_info = json.loads(meta_info["native_info"]) 
            image_list = native_info["image"]  
            word_list = native_info["word"]  
        except KeyError:
            #print "line format error[%s]"%(line[37])
            pass
        except TypeError:
            #print raw_line
            pass
        except ValueError:
            #print raw_line
            pass
        if wap == 1:
            continue
        
            #native_type = native_info["native_config_id"]
        if image_list == None:
            image_list = []
        if word_list == None:
            word_list = []

        image_str = ""
        #for item in sorted(image_list):
        for item in image_list:
            image_type = item["image_type"]
            width = item["width"]
            height = item["height"]
            image_url = item["image_url"]
            image_str += "\002%d_%d_%d"%(image_type,width,height) 
        image_str = image_str.strip("\002")

        word_str = ""
        #for item in sorted(word_list):
        for item in word_list:
            word_type = item["word_type"]
            length = item["length"]
            content = item["content"]
            word_str += "\002%d_%d"%(word_type,length) 
        word_str = word_str.strip("\002")

        fp2_w.write("%s\001%d\001%s\001%s\001%d\001%d\n"%(raw_line.rstrip("\r\n"),native_type,image_str,word_str,freq_cid,freq_adid))

    fp2_w.close()
            

            

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage %s infile nowapfile"%(sys.argv[0])
        sys.exit(1)
    process(sys.argv[1],sys.argv[2])
    
