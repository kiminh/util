#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json
import os
import urllib2

control001 = "192.168.18.13"
remote_seg_dir = "/home/work/debug_request/clean_log/device_redirect_dir/"
path = "data/youku"
post_url = 'http://192.168.18.60:8080/dmp-ad-frontend-server/frontend/get'
files = [ file for file in os.listdir(path) ]
print files

def query_frontend(campaignId):
    fout = open("talkingdata_package/{}".format(campaignId), 'w')
    for file in files:
        absfile = os.path.join(path, file)
        if not os.path.isdir(absfile):
            with open(absfile) as fin:
                print absfile
                cnt = 0 
                match_num = 0 
                for raw_line in fin:
                    line = raw_line.strip("\n\r").split("\001")
                    deviceid = line[0].strip(" ")
                    if deviceid == "": 
                        continue
                    post_data = '{"mediaId":"503", "campaignId": ["%s"], "deviceIdType":66, "deviceId":"%s"}' % (campaignId, deviceid)
                    print post_data
                    req = urllib2.Request(post_url)
                    req.add_header('Content-Type', 'application/json; charset=utf-8')
                    response = urllib2.urlopen(req, post_data)
                    result = json.loads(response.read())
                    try:
                        match = result["result"][0]["matched"]
                    except:
                        print result
                        continue
                    if match:
                        print "deviceid {} matched".format(deviceid)
                        fout.write(deviceid + "\n")
                        match_num += 1
                    cnt+=1
                    if cnt % 1000 == 0:
                        print "process %d" % (cnt)
                    #print result
                print match_num, cnt, match_num * 1.0 / cnt 
    fout.close()

file_path = "/home/work/shuttle_data/bayesbidder.json"
conf_file = open(file_path)
segment_infos = json.load(conf_file)["segmentInfos"]

for line in segment_infos:
    try:
        item = dict(line)
        seg_id = item["id"]
        campaign_id = item["campaign_id"]
        type = int(item["type"])
        #talking data package
        if type == 1:
            query_frontend(campaign_id)
            os.popen("scp work@192.168.18.13:/home/work/debug_request/clean_log/device_redirect_dir/{}".format(campaign_id))
    except:
        print "query fronted error!"
