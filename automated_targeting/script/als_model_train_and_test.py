#coding:utf-8
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
    data_path = sys.argv[1]
    model_path = sys.argv[2]

    sc = SparkContext(
        appName='ling.fang-dmp2.0-normal-tempory-als_train'
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    
    data_df = spark.read.json(data_path)
    (train_data, test_data) = data_df.randomSplit([0.9, 0.1], seed=2019)

    train_data.show(10, False)
    old_test_rmse = 10
    for now_rank in [50]:
        for now_regparam in [0.003,0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01]:
            als = ALS(rank=now_rank, 
                        maxIter=15, 
                        regParam=now_regparam, 
                        userCol="uid_index", 
                        itemCol="pkg_index", 
                        ratingCol="rating")
            als_model = als.fit(train_data)

            predictions_test = als_model.transform(test_data.repartition(50))
            predictions_train = als_model.transform(train_data)
            predictions_test.show()

            evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                predictionCol="prediction")

            print "rank:%s regParam:%s" % (now_rank, now_regparam)
            rmse = evaluator.evaluate(predictions_test.na.drop())
            print("test root mean square error = " + str(rmse))
            rmse2 = evaluator.evaluate(predictions_train.na.drop())
            print("train Root mean square error = " + str(rmse2))
            if rmse < old_test_rmse:
                old_test_rmse = rmse
                als_modol = als.fit(data_df)
                #res = als_model.transform(data_df)
                #res_rmse = evaluator.evaluate(res.na.drop())
                #print "all data root mean square error = %g" % res_rmse
                als_model.userFactors.write.json(model_path + "/userFactors", 'overwrite')
                als_model.itemFactors.write.json(model_path + "/itemFactors", 'overwrite')
                
                #appRecs.write.json("/user/ling.fang/1/2/3/4/5/automated_targeting/test/json", 'overwrite')
                #predictions_test.na.drop().write.json(res_path, "overwrite")
                #predictions_train.na.drop().write.json(res_path, "overwrite")
