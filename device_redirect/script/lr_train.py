#coding:utf-8
"""
Logistic Regression
"""

import sys, math
import numpy as np
from sklearn import metrics

class LRModel:
    def __init__(self, alpha, beta, l2_reg, fea_num, precent, train_ins, result_out):
        self.alpha      = alpha
        self.beta       = beta
        self.fea_num    = fea_num
        self.l2_reg     = l2_reg
        self.precent    = precent
        self.result_out = result_out
        self.epoch      = 1
        self.w          = [ 0.0 for i in range(fea_num+1) ]
        self.n          = [ 0.0 for i in range(fea_num+1) ]
        self.train_ins  = train_ins

    def updateW(self, grad, l, i):
       wi = self.w[i]
       grad = grad * float(l) + self.l2_reg * wi

       self.n[i] += grad * grad
       lr = self.alpha / math.sqrt(self.n[i] + self.beta)
       self.w[i] -= lr * grad

    def updateW0(self, grad):
        w0 = self.w[-1]
        grad = grad + self.l2_reg * w0

        self.n[-1] += grad * grad
        lr = self.alpha / math.sqrt(self.n[-1] + self.beta)
        self.w[-1] -= 0.1 * grad#lr * grad

    def pred_ins(self, line):
        sum = self.w[-1]
        for i, l in enumerate(line[2:]):
            sum += float(l) * self.w[i]
        return self.sigmoid(sum)

    def sigmoid(self, inx):
        return 1.0 / (1.0 + math.exp(-inx))

    def adagrad_train(self):
        for i in range(self.epoch):
            with open(self.train_ins) as fin:
                for k, raw_line in enumerate(fin):
                    line = raw_line.strip("\n\r ").split()
                    label = int(float(line[0]))
                    
                    pred = self.pred_ins(line)
                    grad = pred - label
                    #print grad, label
                    if k % 10000 == 0:
                        print "train ins {}".format(k)

                    for i, l in enumerate(line[2:]):
                        self.updateW(grad, l, i)

                    self.updateW0(grad)
    
    def task_pred(self):
        pred = []
        label = []
        with open(self.train_ins) as fin:
            for raw_line in fin:
                line = raw_line.strip("\r\n ").split()
                p = self.pred_ins(line)
                l = int(float(line[0]))

                pred.append(p)
                label.append(l)
        print "AUC is {}".format(metrics.roc_auc_score(label, pred))
        print "logloss is {}".format(metrics.log_loss(label, pred))

    def get_audience_target(self):        
        pred = {}
        with open(self.train_ins) as fin:
            for raw_line in fin:
                line = raw_line.strip().split()
                if int(line[0]) == 0:
                    p = self.pred_ins(line)
                    deviceid = line[1]
                    if deviceid not in pred:
                        pred[deviceid] = p
        
        ret = sorted(pred.items(), key = lambda x:x[1], reverse=True)
        length = len(pred)
        topk = int(length * self.precent)
        
        with open(self.result_out, 'w') as fout:
            for k in pred.keys()[:topk]:
                fout.write(k + "\n")

    def printW(self):
        print self.w

if __name__ == '__main__':
    
    if len(sys.argv) < 3:
        print "Usage: train.py train_ins result_out"
        exit()
    
    #alpha, beta, l2_reg, fea_num, precent, train_ins, result_out
    lr = LRModel(0.01, 1, 0.01, 80, 0.6, sys.argv[1], sys.argv[2])
    lr.adagrad_train()
    lr.task_pred()
    #lr.printW()
    #lr.get_audience_target()
