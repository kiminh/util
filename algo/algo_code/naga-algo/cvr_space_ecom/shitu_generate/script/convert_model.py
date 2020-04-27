import json
import sys

token2idx = json.load(open(sys.argv[2]))
idx2token = dict((value, key) for key, value in token2idx.items())

f_in = open(sys.argv[1])
next(f_in)
with open(sys.argv[3], 'w') as f_out:
    for i, raw_line in enumerate(f_in):
        line = raw_line.strip()
        f_out.write('%s %s\n' % (idx2token[i], line))

