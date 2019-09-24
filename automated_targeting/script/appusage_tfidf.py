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
    appusage_path = sys.argv[1]
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
    appusage_data_df = spark.read.json(appusage_path)
    appusage_data_df = appusage_data_df.drop('idf')
    
    #appusage_data_df = appusage_data_df.filter(appusage_data_df.ip_city == country)

    N = appusage_data_df.dropDuplicates(['gaid']).count()
    # count idf
    pkg_idf = appusage_data_df.groupBy("package_name") \
        .agg(F.count("gaid").alias("g_cnt")) \
        .withColumn("idf", count_idf_udf(F.lit(N), "g_cnt")) \
        .select("package_name", "idf") \
        .withColumnRenamed("package_name", "pkg")
    pkg_idf.show(10, False)

    user_usage_df = appusage_data_df.groupBy("gaid") \
        .agg(F.sum("use_cnt_all").alias("t_cnt")) \
        .withColumnRenamed("gaid", "g_id")
    user_usage_df.show(10, False)

    user_tf = appusage_data_df.join(user_usage_df, 
        user_usage_df.g_id==appusage_data_df.gaid) \
        .select("gaid", "category", "package_name", "use_cnt_all", "t_cnt") \
        .withColumn("tf", count_tf_udf("use_cnt_all", "t_cnt"))
    user_tf.show(10, False)

    user_tfidf = user_tf.join(pkg_idf, pkg_idf.pkg==user_tf.package_name) \
        .withColumn("tfidf", count_tfidf_udf("tf", "idf")) \
        .select("gaid", "package_name", "category", "tfidf", "idf", "use_cnt_all", "t_cnt")
    
    user_tfidf = user_tfidf.withColumn('use_cnt_all_s', smooth_udf('use_cnt_all'))
    user_tfidf.show(10, False)
    user_tfidf.write.json(des_path + '/json', "overwrite")
