#coding:utf-8
import pandas as pd
import socket
import chardet
import numpy as np
from sklearn import linear_model
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.preprocessing import OneHotEncoder,LabelEncoder
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.ensemble import GradientBoostingClassifier
import jieba,gensim
from sklearn import metrics
from scipy import sparse
import sys
from sklearn.utils import shuffle

doc2vec_size = 150
filter_chars = [" ", "「", "」", "》", "\r", "\n", "\t", "。", "，", ",", "“", "！", "？", "！", "《", ":", "；", "：", "（", "）", "、"]
 
def word_segment(s):
    try:
        for c in filter_chars:
            s = s.replace(c, "")
    except:
        print s
        return ""
    it = jieba.cut(s, cut_all=False)

    _ = []
    for w in it:
        _.append(w)
    result = ' '.join(_)
    return result
    try:
        result = result.encode("gbk")
        return result
    except:
        print "error" + " : " + result
        return ""

header = ["idfa", "screenhight", "screenheight", "carrier", "connectiontype", "model", "model1", "osv", "devicetype", "ip", "title", "keywords", "channel", "cs"]
base_feature = ["screenhight", "screenheight", "carrier", "connectiontype", "model", 'model1', "osv", "devicetype", "ip18", "ip20", "ip24", "ip30", "channel", "cs"]
title_feature = [ 'title_{}'.format(i) for i in range(doc2vec_size) ]

train_data = pd.read_csv(sys.argv[1])
test_data = pd.read_csv(sys.argv[2])

data = pd.concat([train_data, test_data])
data = data.reset_index(drop=True)
data["title"] = data.apply(lambda x: word_segment(x["title"]), axis=1)
print "Word Segment Complete..."
def read_corpus():
    for i, line in enumerate(data["title"]):
        # split with space to isolate each word
        # the words list are tagged with a label as its identity
        #try:
        #print line
        yield gensim.models.doc2vec.TaggedDocument(gensim.utils.to_unicode(line).split(), ['%s' % i])
        #except:
        #    print line
        #    yield gensim.models.doc2vec.TaggedDocument(gensim.utils.to_unicode("this is default").split(), ['%s' % i])

#生成一个train corpus
train_corpus = list(read_corpus())
model = gensim.models.doc2vec.Doc2Vec(vector_size=150, min_count=1, epochs=50, workers=4)
model.build_vocab(train_corpus)
model.train(train_corpus, total_examples=model.corpus_count, epochs=model.iter)
model.save('model/model.dat') 
print "Doc2Vec Train Finished..."
