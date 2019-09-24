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

def count_idf(N, g_cnt):
    idf = math.log((N + 1.0) / (g_cnt + 1.0)) + 1.0
    return idf

def count_tf(cnt, t_cnt):
    if cnt <= 2:
        tf = cnt * 1.0 / t_cnt
    else:
        tf = math.log(cnt) / t_cnt
    return tf

def count_tf_idf(tf, idf):
    return tf * idf

def smooth(use_cnt):
    if use_cnt > 30:
        return 30.0
    else:
        return use_cnt

if __name__ == '__main__':
    user_applist_path = sys.argv[1]
    des_path = sys.argv[2]

    sc = SparkContext(
        appName='ling.fang-dmp2.0-normal-temporary-appusage_tfidf'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    count_idf_udf = F.udf(count_idf, T.FloatType())
    count_tf_udf = F.udf(count_tf, T.FloatType())
    count_tfidf_udf = F.udf(count_tf_idf, T.FloatType())
    smooth_udf = F.udf(smooth, T.FloatType())
    # read appusage info
    user_applist_df = spark.read.json(user_applist_path)
    user_applist_df = user_applist_df.filter(user_applist_df.ip_city == 'US')
    N = user_applist_df.dropDuplicates(['gaid']).count()
    user_applist_df = user_applist_df.select(F.explode('applist').alias('pkg_name'), 'gaid')
    print user_applist_df.count()
    user_applist_df.show()

    # count idf
    #pkg_idf = user_applist_df.groupBy("pkg_name") \
    #    .agg(F.count("gaid").alias("g_cnt")) \
    #    .withColumn("idf", count_idf_udf(F.lit(N), "g_cnt")) \
    #    .select("pkg_name", "idf") \
    #    .withColumnRenamed("pkg_name", "pkg")
    #pkg_idf.show(10, False)

    #user_applist_df = user_applist_df.join(pkg_idf, user_applist_df.pkg_name == pkg_idf.pkg)
    user_applist_df.drop('pkg').write.json(des_path + '/json', 'overwrite')
