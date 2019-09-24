#coding:utf-8
"""
    从用户安装的app里面获取种子用户
    1､学习相似的app
    2､找出装有相似app的用户，（写入记录里面带上app名）
"""
import sys
import math
from pyspark.sql import Window

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark import SparkFiles
import json
import datetime

def euclidean_distance(vector1, vector_str):
    dis = 0.0 
    for v1, v2 in zip(vector1, vector_str.split("_")):
        dis += (v1 - float(v2)) ** 2
    return math.sqrt(dis)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "args lens is less than 2"
        exit(0)
    src_path = sys.argv[1]
    apps_vector_path = sys.argv[2]
    obj_pkg_name = sys.argv[3]
    country = sys.argv[4]
    des_path = sys.argv[5]

    sc = SparkContext(appName="DMP:user_show_click_conv")
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    euclidean_distance_udf = F.udf(euclidean_distance, T.FloatType())

    user_applist_df = spark.read.json(src_path)
    user_applist_df = user_applist_df.filter(user_applist_df.ip_city == country)

    apps_vector_df = spark.read.json(apps_vector_path)
    user_applist_df = user_applist_df.drop('pkg_name') 
    user_applist_df.show()
    apps_vector_df.show()

    user_applist_df = user_applist_df.join(apps_vector_df,
        user_applist_df.app_name == apps_vector_df.pkg_name)
    user_applist_df.show()
    
    obj_pname_df = spark.createDataFrame([(obj_pkg_name, )], ["package_name"])
    obj_pname_df = apps_vector_df.join(obj_pname_df, 
        obj_pname_df.package_name == apps_vector_df.pkg_name)
    
    pkg_vector = obj_pname_df.select('features').collect()[0][0]
    pkg_vector_str = "_".join([str(v) for v in pkg_vector])

    user_applist_df = user_applist_df.withColumn('euc_dis', 
        euclidean_distance_udf('features', F.lit(pkg_vector_str))) 
    user_applist_df = user_applist_df.withColumn('idx',
        F.row_number().over(Window.orderBy(user_applist_df.euc_dis.asc())))
    user_applist_df = user_applist_df.filter(user_applist_df.idx < 12000) \
        .select('gaid', 'app_name')
    
    user_applist_df.dropDuplicates(['app_name']).show(100, False)
    print user_applist_df.count()
    user_applist_df.dropDuplicates(['gaid']).write.json(des_path + '/json', 'overwrite')
