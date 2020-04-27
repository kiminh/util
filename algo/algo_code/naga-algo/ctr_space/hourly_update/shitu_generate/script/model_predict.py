import sys
import math

model_dict = {}
for raw in open(sys.argv[1]):
    felds = raw.strip("\n\r").split(" ")
    if len(felds) < 2:
        continue
    model_dict[felds[0]] = float(felds[1])

y_true = []
y_pred = []
for raw in open(sys.argv[2]):
    s = 0
    felds = raw.strip().split()
    label = felds[0]
    y_true.append(int(label))
    for sp in felds[1:]:
        w = model_dict.get(sp, 0)
        s += model_dict.get(sp, 0)
    y_pred.append(1.0 / (1.0 + math.exp(-s)))
    print "%s %s %s" % (1.0 / (1.0 + math.exp(-s)), label, raw.strip('\n'))
