import sys
import json

if len(sys.argv) < 4:
  print "Usage: get_active_log click_log active_log conversion_log"
  exit(1)

aucid_set = set()

fp_w = open(sys.argv[3], "w")
for raw_line in open(sys.argv[2], "r"):
  aucid_set.add(raw_line.strip("\r\n"))

for raw_line in open(sys.argv[1], "r"):
  line = raw_line.split("\001")
  aucid = line[4]
  flag = 0
  if aucid in aucid_set:
    flag = 1

  fp_w.write("%s\001%s\n"%(flag, raw_line.strip("\r\n")))
fp_w.close()
