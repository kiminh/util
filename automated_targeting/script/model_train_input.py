import sys 
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
 
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row 
from pyspark import SparkContext

from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator

#from pyspark.mllib.linalg import Vectors, VectorUDT
from pyspark.ml.linalg import Vectors, VectorUDT

if __name__ == '__main__':
    
    if len(sys.argv) < 3:
        print "Usage: combine_train_data.py SEED_USER_PATH \
                    USER_INFO_PATH OUT_TRAIN_PATH OUT_TEST_PATH"
        exit()
    fea_vec_path = sys.argv[1]
    out_result_path = sys.argv[2]
    pkg_name = sys.argv[3]

    sc = SparkContext(
            appName='ling.fang-dmp-veryhigh-temporary-Lookalike_model_train'
    )   
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    prob_udf = F.udf(lambda x: float(x[1]), T.FloatType())
    vectorizer_udf = F.udf(lambda vs: Vectors.dense(vs), VectorUDT())

    fea_vector = spark.read.json(fea_vec_path)
    fea_vector.show()
    neg_cnt = fea_vector.filter(fea_vector.label == 0).count()
    pos_cnt = fea_vector.filter(fea_vector.label == 1).count()
    print "positive count is {}, negitive count is {}, ctr is {}" \
        .format(pos_cnt, neg_cnt, pos_cnt * 1.0 /(pos_cnt+neg_cnt))

    fea_vector = fea_vector.withColumn('new_features', 
        vectorizer_udf('features'))

    lr = LogisticRegression(featuresCol="new_features",
                            labelCol="label", 
                            predictionCol="prediction",
                            maxIter=100,
                            regParam=0.01)
    
    grid = ParamGridBuilder() \
        .addGrid(lr.maxIter, [100]) \
        .addGrid(lr.regParam, [0.01]) \
        .build()
    
    evaluator = BinaryClassificationEvaluator() \
        .setMetricName('areaUnderROC')

    cv = CrossValidator(estimator=lr, 
        estimatorParamMaps=grid, 
        evaluator=evaluator, 
        numFolds=3)
    
    #cvModel = cv.fit(fea_vector)
    #lrmodel = cvModel.bestModel
    lrmodel = lr.fit(fea_vector)
    result = lrmodel.transform(fea_vector)
    print "train data AUC: %g" % evaluator.evaluate(result)

    result_user = result.select("gaid", \
        prob_udf(result.probability).alias('prob'), "label", "download_app")

    #filter installed user
    print result_user.count()
    result_user.filter(result_user.label == 1).show(20, False)

    result_user = result_user.filter(result_user.download_app <> pkg_name)
    print result_user.count()
    result_user = result_user.withColumn('idx', \
        F.row_number().over(Window.orderBy(result_user.prob.desc())))
    result_user.select("*").show(10, False)
    result_user.filter(result_user.label == 1).show(20, False)
    
    result_user.write.json(out_result_path + 'json', 'overwrite')
