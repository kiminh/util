import re
import sys 
import json
import math
from datetime import datetime, date
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row 
from pyspark import SparkContext

from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.linalg import SparseVector

from collections import defaultdict

UNK_FEA = 'unk'
EMPTY_FEA = ""
COMBINE_FEA_SEP = '_'
CLICK_THRESHOLD = 50 #for low freq fea filter
TRANS_THRESHOLD = 0

def get_ins(col):
    fea_index_array = col.strip().split()[1:-1]
    return fea_index_array

def get_label(col):
    label = col.strip().split()[0]
    return int(label)

if __name__ == '__main__':
    
    if len(sys.argv) < 3:
        print "Usage: feature_engineer.py FEA_INPUT_PATH \
            FEA_OUTPUT_PATH"
        exit()
    shitu_path = sys.argv[1]
    fea_output_path = sys.argv[2]

    sc = SparkContext(
            appName='Naga dsp cvr feature engineer'
    ) 
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    get_ins_udf = F.udf(get_ins, T.ArrayType(T.StringType()))
    get_label_udf = F.udf(get_label, T.IntegerType())

    #rdd = sc.textFile(shitu_path)
    opt_df = spark.read.text(shitu_path)
    opt_df.show()
    opt_df = opt_df.withColumn('fea_array', get_ins_udf(opt_df.value))
    opt_df = opt_df.withColumn('label', get_label_udf(opt_df.value))  
    opt_df.show()
    opt_df = opt_df.drop('value')
    opt_df.groupby('label').count().show()
    opt_df.show(5, truncate=False)
        
    #topn app feature
    cv = CountVectorizer(binary=True,
        vocabSize=1000000,
        inputCol="fea_array", 
        outputCol="fea_ohe", minDF=10)
    #opt_cv_df = opt_df.select('ifa', 'applist').dropDuplicates()
    cv_model = cv.fit(opt_df)
    
    #save vocabulary for offline mapping 
    voc_df = spark.createDataFrame([Row(features=cv_model.vocabulary)])
    print "vocabulary size:%d " % (len(cv_model.vocabulary))
    voc_df.write.mode('overwrite').json(fea_output_path+"/fea_mapping")
    
    opt_df = cv_model.transform(opt_df)
    opt_df.write.mode('overwrite').json(fea_output_path+"/onehot")

    all_features = ['fea_ohe']
    #all_features = out_ohe_fea
    assmebler = VectorAssembler(inputCols=all_features, outputCol="features")
    feature_vector = assmebler.transform(opt_df) \
        .select("features", "label")
    
    feature_vector.write.parquet(fea_output_path+"/ins", "overwrite")

