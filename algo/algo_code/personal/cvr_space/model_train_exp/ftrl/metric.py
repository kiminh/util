import sys
from sklearn.metrics import roc_auc_score

y_true = []
y_score = []
with open(sys.argv[1]) as f_in:
    for raw_line in f_in:
        line = raw_line.strip("\n\r ").split()
        y_true.append(float(line[1]))
        y_score.append(float(line[0]))

print(sum(y_true), sum(y_score))
print("model copc: %g" % (sum(y_score) / sum(y_true)))
print("model diff: %g" % ((sum(y_score) - sum(y_true)) / sum(y_true)))
print("model auc: %g" % (roc_auc_score(y_true, y_score)))
