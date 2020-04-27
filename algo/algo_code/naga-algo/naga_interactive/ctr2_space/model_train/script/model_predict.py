import sys
import math

model = {}
with open(sys.argv[1]) as f_in:
    for raw in f_in:
        line = raw.strip().split(" ")
        if len(line) < 2:
            continue
        model[line[0]] = float(line[1])

with open(sys.argv[2]) as f_in:
    for raw in f_in:
        wx = 0
        line = raw.strip().split()
        label = int(line[0])
        for fea in line[1:]:
            key = fea.split(":")[0]
            val = float(fea.split(":")[1])
            if key in model:
                wx += model[key]
        pred = 1.0/(1.0+math.exp(-wx)) 
        #pred = math.exp(wx)
        print "%s %s" % (pred, raw.strip())
