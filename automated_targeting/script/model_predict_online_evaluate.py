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

def inner_product(vector1, vector_str):
    rating = 0.0
    for v1, v2 in zip(vector1, vector_str.split("_")):
        rating += v1 * float(v2)
    return rating

if __name__ == '__main__':
    
    factor_path = sys.argv[1]
    test_data_path = sys.argv[2]
    string_index_path = sys.argv[3]
    obj_pkg_name = sys.argv[4]
    usage_path = sys.argv[5]
    des_path = sys.argv[6]

    sc = SparkContext(
        appName='ling.fang-dmp2.0-normal-temporary-als_model_online_evaluate'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    inner_product_udf = F.udf(inner_product, T.FloatType())
    #read user factor df
    userfactor_df = spark.read.json(factor_path + '/userFactors')
    userfactor_df.show()
    itemfactor_df = spark.read.json(factor_path + '/itemFactors')
    itemfactor_df.show()
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

    pkg_si_model = StringIndexerModel.read().load(string_index_path + "/pkg_index")
    uid_si_model = StringIndexerModel.read().load(string_index_path + "/uid_index")
    
    uid_is_model = IndexToString(inputCol="id",
        outputCol="tgaid", labels=uid_si_model.labels)
    user_df = uid_is_model.transform(userfactor_df)

    user_df = user_df.join(F.broadcast(test_df), user_df.tgaid == test_df.gaid)
    user_df.show()    
    print "DSP newsbreak PV = %s" % user_df.count()
    print "DSP newsbreak UV = %s" % user_df.dropDuplicates(['gaid']).count()

    obj_pname_df = spark.createDataFrame([(obj_pkg_name, )], ["package_name"])
    obj_pname_df = pkg_si_model.transform(obj_pname_df)
    pkg_index = obj_pname_df.select(obj_pname_df.pkg_index.cast('int'))
    pkg_index.show()
    
    pkg_vector_df = itemfactor_df.join(pkg_index, 
        itemfactor_df.id == pkg_index.pkg_index)
    pkg_vector_df.show()
    pkg_vector = pkg_vector_df.select('features').collect()[0][0]
    pkg_vector_str = "_".join([str(v) for v in pkg_vector])

    user_df = user_df.withColumn('rating', 
        inner_product_udf('features', F.lit(pkg_vector_str)))
    user_df.show()
    user_df.select("gaid", "rating", "isclick", "isconv") \
        .write.json(des_path + '/json', 'overwrite')  
