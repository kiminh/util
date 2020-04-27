import sys
import math
from sklearn.metrics import roc_auc_score
from sklearn.metrics import log_loss


model_dict = {}
for raw in open(sys.argv[1]):
    felds = raw.strip("\n\r").split(" ")
    if len(felds) < 2:
        continue
    model_dict[felds[0]] = float(felds[1])

y_true = []
y_pred = []
count = 0
for raw in open(sys.argv[2]):
    count += 1
    s = 0
    felds = raw.strip().split()
    label = felds[0]
    y_true.append(int(label))
    for sp in felds[1:]:
        w = model_dict.get(sp, 0)
        s += model_dict.get(sp, 0)
    y_pred.append(1.0 / (1.0 + math.exp(-s))) 

fout = open(sys.argv[3], 'w')
print("model_file: %s" %(sys.argv[1]), file=fout)
print("data_file: %s" %(sys.argv[2]), file=fout)
print("total sample number: %s" % ( count), file=fout)
print("sum y_pred: %s, sum y_true: %s" % (sum(y_pred), sum(y_true)), file=fout)
print("model copc: %g" % (sum(y_pred) / sum(y_true)), file=fout)
print("model diff: %g" % ((sum(y_pred) - sum(y_true)) / sum(y_true)), file=fout)
print("model auc: %g" % (roc_auc_score(y_true, y_pred)), file=fout)
print("logloss: %g" % (log_loss(y_true, y_pred)), file=fout)
fout.close()
