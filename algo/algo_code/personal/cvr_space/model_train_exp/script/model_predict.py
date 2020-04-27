import sys
import math

model_dict = {}
for raw in open(sys.argv[1]):
    felds = raw.strip("\n\r").split(" ")
    if len(felds) < 2:
        continue
    model_dict[felds[0]] = float(felds[1])


for raw in open(sys.argv[2]):
    s = 0
    for sp in raw.strip().split()[1:]:
        w = model_dict.get(sp, 0)
        s += model_dict.get(sp, 0)
    print 1.0 / (1.0 + math.exp(-s))    
