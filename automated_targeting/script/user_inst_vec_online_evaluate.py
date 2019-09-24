#coding:utf-8
import sys
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import Window
 
from pyspark.ml.feature import Bucketizer
from pyspark.ml.feature import StringIndexer, IndexToString, StringIndexerModel

import math

def euclidean_distance(vector1, vector_str):
    dis = 0.0
    for v1, v2 in zip(vector1, vector_str.split("_")):
        dis += (v1 - float(v2)) ** 2
    return math.sqrt(dis)

if __name__ == '__main__':
    test_data_path = sys.argv[1]
    user_ins_path = sys.argv[2]
    apps_vec_path = sys.argv[3]
    obj_pkg_name = sys.argv[4]
    des_path = sys.argv[5]

    sc = SparkContext(
        appName='ling.fang-dmp2.0-normal-temporary-als_model_online_evaluate'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    euclidean_distance_udf = F.udf(euclidean_distance, T.FloatType())
 
    test_df = spark.read.json(test_data_path)
    test_df = test_df.filter((test_df.time > 20190601000000) & 
        (test_df.bundle_id == "com.cootek.smartinputv5") &
        (test_df.promoted_app == "com.particlenews.newsbreak"))
    #test_df = test_df.filter((test_df.time > 20190601000000) & 
    #    (test_df.bundle_id == "com.cootek.smartinputv5") &
    #    (test_df.promoted_app == "com.machsystem.gawii"))
    test_df.show()
    print test_df.count()
    print test_df.dropDuplicates(['gaid']).count()
    
    apps_vect_df = spark.read.json(apps_vec_path)
    apps_vect_df.show()
    user_vect_df = spark.read.json(user_ins_path)
    user_vect_df = user_vect_df.withColumnRenamed('gaid', 'tgaid')
    user_vect_df.show()
    
    
    test_df = test_df.join(user_vect_df, 
        test_df.gaid == user_vect_df.tgaid)
    test_df = test_df.drop('tgaid')   
    print test_df.count()
    print test_df.dropDuplicates(['gaid']).count()

    obj_pname_df = spark.createDataFrame([(obj_pkg_name, )], ["package_name"])
    obj_pname_df = obj_pname_df.join(apps_vect_df, 
        obj_pname_df.package_name == apps_vect_df.pkg_name)
    
    pkg_vector = obj_pname_df.select('features').collect()[0][0]
    pkg_vector_str = "_".join([str(v) for v in pkg_vector])

    test_df = test_df.withColumn('rating', 
        euclidean_distance_udf('user_vect', F.lit(pkg_vector_str)))
    test_df.show()
    test_df.select("gaid", "rating", "isclick", "isconv").write.json(des_path + '/json', 'overwrite')
