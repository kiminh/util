#!/bash/bin
HADOOP_HOME="/usr/local/hadoop-2.6.3"
USER_NAME='ad_user'
VERSION=`date +%Y%m%d%H%M%S`
file_time_flag=`date -d " 1 days ago " +%Y%m%d`

bash -x shell/feature_engineer.sh
if [[ $? -ne 0 ]];then
    #python utils/sms_sender.py "feature engineer $VERSION error!"
    #python utils/email_sender.py "spark version cvr model train" "feature engineer $VERSION error!"
    exit 1
fi
bash -x shell/run_model_train.sh $file_time_flag
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model train $VERSION error!"
    python utils/email_sender.py "spark version cvr model train" "feature engineer $VERSION error!"
    exit 1
fi

$HADOOP_HOME/bin/hadoop fs -cat /user/$USER_NAME/ocpc/model_train/extract_ins_fea/$file_time_flag/fea_mapping/* > ./model/fea_mapping.json
$HADOOP_HOME/bin/hadoop fs -cat /user/$USER_NAME/ocpc/model_train/model_output/$file_time_flag/* > ./model/model_dict.json

model_file='./model/lr_model.dat'
model_bk=./model_bk/
python script/convert_model.py ./model/model_dict.json ./model/fea_mapping.json ${model_file}.new
if [[ $? -ne 0 ]];then
    echo "convert model error!"
    exit 1
fi

mv $model_file $model_bk/lr_model.dat.$VERSION
mv $model_file.new $model_file

$HADOOP_HOME/bin/hadoop fs -cat /user/$USER_NAME/ocpc/model_train/shitu/appclktrans/$file_time_flag/* > ./model/calibrate.dat

python script/model_push_util.py $VERSION conf/model_push.conf
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model push online $VERSION error!"
    python utils/email_sender.py "online model train" "model push online $VERSION error!" 
    exit 1
fi

python utils/sms_sender.py "[$VERSION] ftrl cvr model train success!"
exit 0
