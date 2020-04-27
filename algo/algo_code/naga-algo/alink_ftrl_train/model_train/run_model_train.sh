#!/bash/bin
#FLINK_SUMBIT_SHELL="/usr/local/flink_apd_submit.sh"
FLINK_SUMBIT_SHELL="/usr/local/flink_bdp_submit.sh"
ROOT_DIR=`pwd`

#train:-p 64 -ys 2 -yjm 8g -ytm 32g
#-p 并行度
#-ys Number of slots per TaskManager,相当于spark core， executor=p/ys
#-yjm drive内存
#-ytm executor内存
#详细参考 https://www.kancloud.cn/yue23yue/flink/812516

flink_submit="bash ${FLINK_SUMBIT_SHELL} \
                 -p 64 -ys 2 -yjm 8g -ytm 32g -m yarn-cluster \
                 -yqu root.ad-root.etl.dailyetl.high \
                 -ynm alink_ftrl_train_sample_0.01 \
                 -j /usr/local/flink-pipeline_1.10.jar"

$flink_submit -py $ROOT_DIR/model_train.py -pyfs $ROOT_DIR/feature_engineer.py
