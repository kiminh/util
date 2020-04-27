import mmh3
import sys

print mmh3.hash("%s:%s" % (sys.argv[1], sys.argv[2].lower()), signed=False)
