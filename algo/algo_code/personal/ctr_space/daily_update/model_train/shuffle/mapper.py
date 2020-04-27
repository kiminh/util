import sys
import random
for raw_line in sys.stdin:
    key = random.randint(0, sys.maxsize)
    print "%s\t%s" % (key, raw_line.strip())    
