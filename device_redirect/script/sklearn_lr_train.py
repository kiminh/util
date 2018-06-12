#coding:utf-8
import sys
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics

import jieba,gensim

def shuffle(X, y):
   m = X.shape[0]
   for i in range(7):
       ind = np.arange(m)
   np.random.shuffle(ind)
   return X[ind], y[ind]

def read_data(filename):
    X = []
    Y = []
    with open(filename) as fin:
        for raw_line in fin:
            line = raw_line.strip(" \n\r").split(" ")
            Y.append(int(line[0]))
            x = []
            for l in line[2:]:
                x.append(float(l))
            X.append(x)
    return np.array(X), np.array(Y)

def train_and_predict(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)
    classifier = LogisticRegression()
    classifier.fit(X_train, y_train)
    pred = classifier.predict_proba(X_test)[:,1]
    pred1 = classifier.predict_proba(X)[:,1]
    print "test AUC is {}".format(metrics.roc_auc_score(y_test, pred))
    print "train AUC is {}".format(metrics.roc_auc_score(y, pred1))
    print "train logloss is {}".format(metrics.log_loss(y, pred1))

if __name__ == '__main__':
    
    if len(sys.argv) < 2:
        print "Usage: train.py train_ins"
        exit()

    print "Read Data From File ..."
    X, y = read_data(sys.argv[1])
    print "Shuffle Train Instance ..."
    X, y = shuffle(X, y)
    print "LR Train and Score ..."
    train_and_predict(X, y)
