import sys
import random

for raw_line in sys.stdin:
    line_sp = raw_line.strip("\n\r ").split("\t")
    if len(line_sp) < 2:
        continue
    #if random.random() < 0.7:
    #   print line_sp[1]
    print line_sp[1]
