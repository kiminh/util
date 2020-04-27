import sys
from sklearn.metrics import roc_auc_score

y_true = []
y_score = []
click = 0
trans = 0
for raw_line in sys.stdin:
    line = raw_line.strip("\n\r ").split()
    y_true.append(float(line[1]))
    y_score.append(float(line[0]))
    if int(line[1]) == 1:
        trans+=1
    click+=1

diff = (sum(y_score) - sum(y_true)) / sum(y_true)
auc = roc_auc_score(y_true, y_score)
print ("diff: %g, auc: %g, trans: %d, click: %d" % (diff, auc, trans, click))
