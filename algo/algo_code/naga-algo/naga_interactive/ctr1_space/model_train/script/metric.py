import sys
from sklearn.metrics import roc_auc_score

y_true = []
y_score = []
click_list = []
for raw_line in sys.stdin:
    line = raw_line.strip("\n\r ").split()
    y_true.append(float(line[1]))
    y_score.append(float(line[0]))
    if int(line[1]) == 1:
        click_num = int(line[3].split(":")[1])
        click_list.append(click_num)
    

print(sum(y_true), sum(y_score))
print(sum(click_list), sum(y_score))
print("model copc1: %g" % (sum(y_score) / sum(click_list))) 
print("model copc: %g" % (sum(y_score) / sum(y_true)))
print("model diff: %g" % ((sum(y_score) - sum(y_true)) / sum(y_true)))
print("model auc: %g" % (roc_auc_score(y_true, y_score)))
