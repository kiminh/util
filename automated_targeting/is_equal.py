import json
import sys

thres = int(sys.argv[3])
set1 = set()
set2 = set()
with open(sys.argv[1]) as fin1, open(sys.argv[2]) as fin2:
    for i, raw_line in enumerate(fin1):
        if i < thres:
            set1.add(raw_line.strip("\n\r"))
        else:
            break
    for i, raw_line in enumerate(fin2):
        if i < thres:
            set2.add(raw_line.strip("\n\r"))
        else:
            break
    res = (set1 & set2)
    print res
    print len(res)
