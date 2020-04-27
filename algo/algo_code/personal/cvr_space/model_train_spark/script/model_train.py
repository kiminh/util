import sys 
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
 
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row 
from pyspark import SparkContext

from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator

from datetime import datetime, date, timedelta
if __name__ == '__main__':
    
    if len(sys.argv) < 3:
        print "Usage: combine_train_data.py SEEK_USER_PATH \
                    USER_INFO_PATH OUT_TRAIN_PATH OUT_TEST_PATH"
        exit()
    fea_vec_path = sys.argv[1]
    out_result_path = sys.argv[2]
    #latest_day = sys.argv[3]
    sc = SparkContext(
            appName='Ranger DSP: cvr model Train'
    )   
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    prob_udf = F.udf(lambda x: float(x[1]), T.FloatType())
    label_udf = F.udf(lambda x: int(x), T.IntegerType())

    fea_vector = spark.read.parquet(fea_vec_path)
    #print "total_count:", fea_vector.select('timestamp').count()
    #fea_vector = fea_vector.withColumn('date', F.substring(fea_vector.timestamp,1,8))
    
    #remain ins  sampling
    #auction_ins = fea_vector.filter(fea_vector.launch_type == '3')
    #(remain_ins_for_train, remain_ins_not_train) = fea_vector.filter(fea_vector.launch_type == '2').randomSplit([0.1, 0.9])
    #print "auction_ins: %d" % (auction_ins.count())
    #print "remain_ins_not_train: %d" % (remain_ins_not_train.count())
    #print "remain_ins_for_train: %d" % (remain_ins_for_train.count())
    #fea_vector = auction_ins.union(remain_ins_for_train)
    #fea_vector = auction_ins

    
    (train_data, test_data) = fea_vector.randomSplit([0.8, 0.2])
    
    train_data.show()
    test_data.show()
    neg_cnt = fea_vector.filter(fea_vector.label == 0).count()
    pos_cnt = fea_vector.filter(fea_vector.label == 1).count()
    print "positive count is {}, negitive count is {}, ctr is {}" \
        .format(pos_cnt, neg_cnt, pos_cnt * 1.0 /(pos_cnt+neg_cnt))
    
    print "generate train/test ins success!"
    lr = LogisticRegression(featuresCol="features",
        labelCol="label", 
        predictionCol="prediction")
    
    #print "lr params:"
    #print lr.explainParams()
    #.addGrid(lr.regParam, [0.01, 0.05, 0.1]) \
    #.addGrid(lr.regParam, [0.05]) \
    #.addGrid(lr.elasticNetParam, [0.0]) \
    
    #.addGrid(lr.regParam, [0.1]) \
    #.addGrid(lr.elasticNetParam, [0.0]) \
    grid = ParamGridBuilder() \
        .addGrid(lr.maxIter, [50]) \
        .addGrid(lr.regParam, [0.05]) \
        .build()
    
    #print "config: netparam:%s\tregparam:%s" % (str(lr.getOrDefault('elasticNetParam')), str(lr.getOrDefault('regParam')))
    
    evaluator = BinaryClassificationEvaluator()
    cv = CrossValidator(estimator=lr, 
        estimatorParamMaps=grid, 
        evaluator=evaluator, 
        numFolds=3)
    
    cvModel = cv.fit(train_data)
    lrmodel = cvModel.bestModel
    
    '''
    print "bestmodel para select:"
    print lrmodel.explainParams()
    
    print lrmodel.explainParam(lrmodel.regParam)
    print lrmodel.explainParam(lrmodel.elasticNetParam)
    '''

    lrsum = lrmodel.evaluate(train_data)
    auc = lrsum.areaUnderROC
    print "train auc {}".format(auc)
    
    '''
    precision = lrsum.precisionByThreshold
    recall = lrsum.recallByThreshold
    print "train_precision:"
    precision.select('*').show()
    print "train_recall:"
    recall.select('*').show()
    '''

    lrsum = lrmodel.evaluate(test_data)
    auc = lrsum.areaUnderROC
    print "test auc {}".format(auc)
    
    '''
    precision = lrsum.precisionByThreshold
    recalll = lrsum.recallByThreshold
    print "test_precision:"
    precision.select('*').show()
    print "test_recall:"
    recall.select('*').show()
    '''

    result = lrmodel.transform(test_data)
    result = result.withColumn('prob', prob_udf(result.probability))
    result.write.mode('overwrite').json(out_result_path+"testprob/")
    
    reg_eval = RegressionEvaluator(predictionCol="prob")
    metric = reg_eval.evaluate(result)
    print "metric_mse:", metric
    #print "metric_mean absolute error:", reg_eval.evaluate(result, {evaluator.metricName: "mae"})
    #print "metric_mean r^2 error:", reg_eval.evaluate(result, {evaluator.metricName: "r2"})
    result_clc = result.filter(result.label == 1).count()
    result_pclc = result.agg(F.sum(result.prob)).collect()[0][0]
    print 'result_pclc: %s, result_clc: %s, copc: %s' % (result_pclc * 1.0/ result_clc, result_pclc, result_clc) 

    coef_vec = lrmodel.coefficientMatrix
    intercept_vec = lrmodel.interceptVector
    model_df = spark.createDataFrame([(coef_vec, intercept_vec)], ['coefficientMatrix', 'interceptVector'])
    model_df.write.mode('overwrite').json(out_result_path)
    #lrmodel.write().overwrite().save(out_result_path)

    sys.exit(0)

