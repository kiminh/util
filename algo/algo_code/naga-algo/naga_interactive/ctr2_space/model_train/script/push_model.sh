#!/bash/bin
ver=`date +%Y%m%d%H%M%S`
python script/model_push_util.py $ver conf/model_push.conf
