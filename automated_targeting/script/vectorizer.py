#coding:utf-8
import os
import sys
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.ml.feature import Bucketizer

if __name__ == '__main__':
    usage_tfidf_path = sys.argv[1]
    si_model_path = sys.argv[2]
    des_path = sys.argv[3]
    print si_model_path
    sc = SparkContext(
        appName='ling.fang-dmp2.0-normal-tempory-vectorize'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    # read train and test data
    user_appusage_df = spark.read.json(usage_tfidf_path)
    user_appusage_df = user_appusage_df.filter(user_appusage_df.gaid != "")
    user_appusage_df.show()
    # userid index
    si_model = StringIndexer(inputCol="gaid", outputCol="uid_index") \
        .setHandleInvalid("skip").fit(user_appusage_df)
    user_appusage_df = si_model.transform(user_appusage_df)
    si_model.write().overwrite().save(si_model_path + "/uid_index")
        
    # appname index 
    si_model = StringIndexer(inputCol="package_name", outputCol="pkg_index") \
        .setHandleInvalid("skip").fit(user_appusage_df)
    user_appusage_df = si_model.transform(user_appusage_df)
    si_model.write().overwrite().save(si_model_path + "/pkg_index")

    #user_appusage_df = user_appusage_df.withColumnRenamed('tfidf', 'rating')
    user_appusage_df = user_appusage_df.withColumnRenamed('use_cnt_all_s', 'rating')
    user_appusage_df.write.json(des_path + '/json', 'overwrite')
