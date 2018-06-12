#coding:utf-8
import pandas as pd
import numpy as np
import jieba, gensim
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

doc2vec_size = 300
title_feature = [ 'title_{}'.format(i) for i in range(doc2vec_size) ]

def word_segment(s):
    it = jieba.cut(s, cut_all=False)
    _ = []
    for w in it:
        _.append(w)
    result = ' '.join(_)
    try:
        result = result.encode("gbk")
        return result
    except:
        print result
        return ""

def read_data(seek_file, target_file):
    train_data = pd.read_csv(seek_file, names=['idfa', 'title', 'channel'])
    test_data = pd.read_csv(target_file, names=['idfa', 'title', 'channel'])

    train_data["is_seek"] = 1
    test_data["is_seek"] = 0

    data = pd.concat([train_data, test_data])
    data = data.reset_index(drop=True)
    data['title'] = data.apply(lambda x: word_segment(x['title']), axis=1)
    #data['channel'] = data.apply(lambda x: word_segment(x['channel']), axis=1)
    
    return data

def read_corpus(data):
    for i, line in enumerate(data['title']):
        # split with space to isolate each word
        # the words list are tagged with a label as its identity
        try:
            yield gensim.models.doc2vec.TaggedDocument(gensim.utils.to_unicode(line.decode("GB2312")).split(), ['%s' % i])
        except:
            print line
            yield gensim.models.doc2vec.TaggedDocument(gensim.utils.to_unicode("this is default").split(), ['%s' % i])

def doc2vec_train(data):
    train_corpus = list(read_corpus(data))
    
    model = gensim.models.doc2vec.Doc2Vec(vector_size=doc2vec_size, min_count=1, epochs=50, workers=7)
    model.build_vocab(train_corpus)
    model.train(train_corpus, total_examples=model.corpus_count, epochs=model.iter)

    return model    

def write_train_ins(model, data, out_file):
    f_out = open(out_file, 'w')
    docvec = []
    
    ind = np.arange(data.shape[0])
    np.random.shuffle(ind)
    for i in ind:
        row = []
        f_out.write(str(data['is_seek'][i]) + " " + str(data['idfa'][i]) + " ")
        for idx in range(doc2vec_size):
            f_out.write(str(model[i][idx]) + " ")
        f_out.write("\n")
    f_out.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print "Usage: python generate_ins.py seek_file target_file train_ins"
        exit()
    print "Read data and word segment ..."
    data = read_data(sys.argv[1], sys.argv[2])
    print "doc2vec train ..."
    model = doc2vec_train(data)
    print "doc2vec finished ..."
    print "write train instance to file ..."
    write_train_ins(model, data, sys.argv[3])
