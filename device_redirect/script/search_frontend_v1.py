import urllib2
import json
import os
import sys
import time

if len(sys.argv) < 3:
    print "Usage : python search_ss.py campaignId output"
    exit()

path = "data/youku"
post_url = 'http://192.168.18.60:8080/dmp-ad-frontend-server/frontend/get'
files = [ file for file in os.listdir(path) ]
print files

fout = open(sys.argv[2], 'w')

for file in files:
    absfile = os.path.join(path, file)
    if not os.path.isdir(absfile):
        with open(absfile) as fin:
            print absfile
            cnt = 0
            match_num = 0
            for raw_line in fin:
                line = raw_line.strip("\n\r").split(",")
                deviceid = line[0].strip(" ")
                if deviceid == "":
                    continue
                post_data = '{"mediaId":"503", "campaignId": ["%s"], "deviceIdType":66, "deviceId":"%s"}' % (sys.argv[1], deviceid)
                req = urllib2.Request(post_url)
                req.add_header('Content-Type', 'application/json; charset=utf-8')
                response = urllib2.urlopen(req, post_data)
                result = json.loads(response.read())
                try:
                    match = result["result"][0]["matched"]
                except:
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
