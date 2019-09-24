#coding:utf-8
import sys
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.ml.feature import Bucketizer
import math

def vector_weight(vect, idf):
    vect_w = []
    for v in vect:
        vect_w.append(v * idf)
    return vect_w

def vector_sum(vect_list):
    size = len(vect_list[0])
    vec_sum = [ 0 for i in range(size) ]
    for vect in vect_list:
        for i, v in enumerate(vect):
            vec_sum[i] += v
    return vec_sum

def vector_mean(vector_sum, idf_sum):
    vect_mean = []
    for v in vector_sum:
        vect_mean.append(v / idf_sum)
    return vect_mean

if __name__ == '__main__':
    appusage_path = sys.argv[1]
    apps_vect_path = sys.argv[2]
    seed_user_path = sys.argv[3]
    des_path = sys.argv[4]

    sc = SparkContext(
        appName='ling.fang-dmp2.0-normal-temporary-appusage_tfidf'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    vector_weight_udf = F.udf(vector_weight, T.ArrayType(T.FloatType()))
    vector_sum_udf = F.udf(vector_sum, T.ArrayType(T.FloatType()))
    vector_mean_udf = F.udf(vector_mean, T.ArrayType(T.FloatType())) 

    # read appusage info
    appusage_data_df = spark.read.json(appusage_path)
    apps_vect_df = spark.read.json(apps_vect_path)
    apps_vect_df = apps_vect_df.drop('category')
    seed_user_df = spark.read.json(seed_user_path)
    seed_user_df = seed_user_df.withColumnRenamed('gaid', 'tgaid')

    print appusage_data_df.dropDuplicates(['gaid']).count()

    appusage_data_df.show()
    apps_vect_df.show()

    appusage_data_df = appusage_data_df.join(apps_vect_df, \
        appusage_data_df.package_name == apps_vect_df.pkg_name)

    appusage_data_df = appusage_data_df.withColumn('features_weight',
        vector_weight_udf('features', 'tfidf'))
 
    appusage_data_df = appusage_data_df.groupBy('gaid').agg(
        vector_sum_udf(F.collect_list('features_weight')).alias('features_sum'),
        F.sum('tfidf').alias('tfidf_sum'))
    appusage_data_df = appusage_data_df.withColumn('features', 
        vector_mean_udf('features_sum', 'tfidf_sum'))
    appusage_data_df.show()
    appusage_data_df = appusage_data_df.select('gaid', 'features')
    print "seed user cnt: %s" % seed_user_df.count()
 
    appusage_data_df = appusage_data_df.join(seed_user_df, 
        appusage_data_df.gaid == seed_user_df.tgaid, 'left')
    
    appusage_data_df = appusage_data_df.withColumn('label',
        F.when(appusage_data_df.tgaid.isNull(), 0).otherwise(1)) \
        .withColumn('download_app', F.when(appusage_data_df.app_name.isNull(), "unknown") \
        .otherwise(appusage_data_df.app_name))

    print "smaple join seed user cnt: %s" % appusage_data_df.filter(appusage_data_df.label == 1).count()
    appusage_data_df.select('gaid', 'features', 'label', 'download_app').write.json(des_path + '/json', "overwrite")
