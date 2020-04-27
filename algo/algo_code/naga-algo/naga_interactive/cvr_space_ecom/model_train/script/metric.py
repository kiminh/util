import sys
from sklearn.metrics import roc_auc_score

y_true = []
y_score = []
ed = 0
for raw_line in sys.stdin:
    line = raw_line.strip("\n\r ").split()
    y_true.append(float(line[1]))
    y_score.append(float(line[0]))
    ed += 1

print("ed:%s, click:%s, pclick:%s" % (ed, sum(y_true), sum(y_score)))
print("model copc: %g" % (sum(y_score) / sum(y_true)))
print("model diff: %g" % ((sum(y_score) - sum(y_true)) / sum(y_true)))
print("model auc: %g" % (roc_auc_score(y_true, y_score)))
