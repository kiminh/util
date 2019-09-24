#coding:utf-8
"""
    创建lookalike样本
    为用子用户标记lable = 1,其他用户标记label = 0
    特征主要是以applist每一个app的vector加权平均
"""
import sys
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark import SparkFiles
import json
import datetime

def vector_sum(vect_list):
    size = len(vect_list[0])
    vec_sum = [ 0 for i in range(size) ]
    for vect in vect_list:
        for i, v in enumerate(vect):
            vec_sum[i] += v
    return vec_sum

def vector_mean(vector_sum, s):
    vect_mean = []
    for v in vector_sum:
        vect_mean.append(v / s)
    return vect_mean

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "args lens is less than 2"
        exit(0)
    src_path = sys.argv[1]
    apps_vector_path = sys.argv[2]
    seed_user_path = sys.argv[3]
    country = sys.argv[4]
    des_path = sys.argv[5]

    sc = SparkContext(appName="DMP:user_show_click_conv")
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    vector_sum_udf = F.udf(vector_sum, T.ArrayType(T.FloatType()))
    vector_mean_udf = F.udf(vector_mean, T.ArrayType(T.FloatType()))

    user_applist_df = spark.read.json(src_path)
    apps_vector_df = spark.read.json(apps_vector_path)
    user_applist_df = user_applist_df.drop('pkg_name') 
    
    user_applist_df.show()
    apps_vector_df.show()

    user_applist_df = user_applist_df.dropDuplicates(['gaid'])
    user_applist_df = user_applist_df.filter(user_applist_df.ip_city == country)

    user_applist_df = user_applist_df.select(F.explode('applist').alias('app_name'), 'gaid', 'ip_city')
    user_applist_df = user_applist_df.join(apps_vector_df,
        user_applist_df.app_name == apps_vector_df.pkg_name)
    
    user_applist_df = user_applist_df.groupBy(['gaid']).agg(
        vector_sum_udf(F.collect_list('features')).alias('features_sum'),
        F.count("*").alias('cnt'),
        F.collect_list('app_name').alias('applist'))

    user_applist_df = user_applist_df.withColumn('features', 
        vector_mean_udf('features_sum', 'cnt'))
    user_applist_df.show()

    seed_user_df = spark.read.json(seed_user_path)
    seed_user_df.show()
    print "seed user count: %s" % seed_user_df.count()
    seed_user_df = seed_user_df.withColumnRenamed('gaid', 'tgaid')
    user_applist_df = user_applist_df.join(seed_user_df, 
        user_applist_df.gaid == seed_user_df.tgaid, 'left')
    
    user_applist_df = user_applist_df.withColumn('label',
         F.when(user_applist_df.tgaid.isNull(), 0).otherwise(1)) \
        .withColumn('download_app', F.when(user_applist_df.app_name.isNull(), "unknown") \
        .otherwise(user_applist_df.app_name))

    user_applist_df = user_applist_df.select('gaid', 'label', 'features', 'download_app', 'applist')
    print user_applist_df.count()

    user_applist_df.write.json(des_path + '/json', 'overwrite')
