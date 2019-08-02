bayescom ftrl model
# bayesalgo

##特征提取
1.cd ./adfea
2.修改conf/run.conf,包括schema、特征列表，输入和输出。
3.运行方式: ./bin/adfea conf/run.conf


##模型训练
1.cd ./ftrl
2.修改conf/ftrl.conf，包括输入输出和参数。
3.运行方式: ./bin/train hashnum conf/ftrl.conf


##评估
1.cd ./ftrl
2.修改conf/ftrl.conf，包括模型输入、评估集合输入和输出结果。
3.运行方式: ./bin/predict conf/ftrl.conf
4.python cal_metric.py 输出结果


##日志拼接
get-log


##特征提取
adfea_control





