import urllib2
import json
import os
import sys
import time
import threading

path = "./data/youku"
path = "./"
post_url = 'http://180.76.243.230:8080/dmp-ad-frontend-server/frontend/get'
files = os.listdir(path)
print files
num_files = len(files)

def handle_file(files, start, end, threadid):
    for i in range(start, end):
        file = files[i]
        absfile = os.path.join(path, file)
        if not os.path.isdir(absfile):
            with open(absfile) as fin:
                print absfile
                out_file = "./data/filter/youku/filter.{}".format(file)
                fout = open(out_file, 'w')
                cnt = 0
                for raw_line in fin:
                    line = raw_line.strip("\n\r").split(" ")
                    idfa = line[0]
                    post_data = '{"mediaId":"503", "campaignId": ["nJ6cRdaw"], "deviceIdType":"67", "deviceId":"%s"}' % (idfa)
                    req = urllib2.Request(post_url)
                    req.add_header('Content-Type', 'application/json; charset=utf-8')
                    response = urllib2.urlopen(req, post_data)
                    result = json.loads(response.read())
                    match = result["result"][0]["matched"]
                    print result
                    if match:
                        print "idfa matched {}".format(idfa)
                        fout.write(raw_line+"\n")
                    cnt+=1
                    if cnt % 1000 == 0:
                        print "%d thread process %d" % (threadid, cnt)
                fout.close()

threads = []
num_threads = 4
start = 0
ava_num = num_files / num_threads

for i in range(num_threads):
    print start, start + ava_num
    t = threading.Thread(target=handle_file, args=(files, start, start + ava_num, i))
    start = start + ava_num
    threads.append(t)

for i in range(num_threads):
    threads[i].start()

for i in range(num_threads):
    threads[i].join()  
