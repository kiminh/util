#coding:utf-8
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

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":

    applist_path = sys.argv[1]
    uid2gaid_path = sys.argv[2]
    des_path = sys.argv[3]

    sc = SparkContext(appName="ling.fang-dmp-normal-daily-get_user_applist")

    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    uid2gaid = spark.read.parquet(uid2gaid_path) \
        .withColumnRenamed('uid', 'tuid') \
        .dropDuplicates(['tuid'])
    print uid2gaid.count()

    #user install applist info
    user_installapp_df = spark.read.parquet(applist_path)
    print user_installapp_df.select("*").take(11)
    user_installapp_source = user_installapp_df \
        .filter(user_installapp_df.is_appearing == "true") \
        .select("uid", "package_name")
    user_installapp_source.show(10, False)

    user_installapp_source = user_installapp_source.join(uid2gaid,
        user_installapp_source.uid == uid2gaid.tuid)    
    
    applist_agg = user_installapp_source.groupBy(["gaid", "ip_city"]) \
        .agg(F.collect_set("package_name").alias("applist"))
   
    print applist_agg.show(10)
    print applist_agg.count()
    applist_agg.write.json(des_path + '/json', 'overwrite')
