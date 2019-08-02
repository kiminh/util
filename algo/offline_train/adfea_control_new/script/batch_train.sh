#!/bin/bash
source ./script/functions.sh

start_time_stamp=$(date -d "-1 day" +%Y%m%d%H)
end_time_stamp=$(date -d "-1 hour"  +%Y%m%d%H)
echo $start_time_flag
echo $end_time_flag

[[ ! -e model ]] && mkdir model
[[ ! -e model_bk ]] && mkdir model_bk
[[ ! -e check_result ]] && mkdir check_result
[[ ! -e history_adfea ]] && mkdir history_adfea
[[ ! -e incre_adfea ]] && mkdir incre_adfea
[[ ! -e key_filter ]] && mkdir key_filter
[[ ! -e notifier ]] && mkdir notifier

root_path=`pwd`
done_path=${root_path}/done_path
batch_adfea_done="${done_path}/adfea_ssp_done.last"
[[ ! -f ${batch_adfea_done} ]] && echo "adfea batch done not exist " && exit 0
source ${batch_adfea_done}

#get log
batch_shitu_log="./history_adfea/shitu.log"
:> ${batch_shitu_log}
for file in `ls $data_path/shitu_[0-9]*`;do

    file_basename=`basename ${file}`
    result=`echo "${file_basename}" | sed -n '/wap$/p'`
    if [ -n "${result}" ];then
        continue
    fi  

    timeflag=${file_basename:0-10}
    
    if (( timeflag > start_time_stamp && timeflag < end_time_stamp ));then
         cat ${file} >> ${batch_shitu_log}
     fi
done

#adfea
adfea_conf="./conf/run_batch.conf"
adfea_conf_temp="./conf/run_temple.conf"
adfea_fea_conf="./conf/fea_list.conf"
adfea_train_file=${batch_shitu_log}
adfea_train_instance="./history_adfea/train_ins"
cp ${adfea_conf_temp} ${adfea_conf}
echo "fea_conf=${adfea_fea_conf}" >> ${adfea_conf}
echo "train_file=${adfea_train_file}" >> ${adfea_conf}
echo "train_instance=${adfea_train_instance}" >> ${adfea_conf}

./bin/adfea ${adfea_conf}
if [ $? -ne 0 ];then
    echo "adfea fail"
    exit
fi

INFO="[ `date -d "0 hour"  +%Y-%m-%d:%H:%M:%S` ] adfea done."
python notifier/dingding_notifier.py $INFO

./bin/shuffle ${adfea_train_instance} ${adfea_train_instance}.shuffle 2000000
if [[ $? -ne 0 ]];then
  echo "shuffle error"
  exit 1
fi

INFO="[ `date -d "0 hour"  +%Y-%m-%d:%H:%M:%S` ] shuffle done."
python notifier/dingding_notifier.py $INFO

#key filter
./bin/key_filter ${adfea_train_instance}.shuffle ${adfea_train_instance}.kf \
                  bloom_file=./key_filter/bloom_filter less_count=2 is_first_start=1
if [[ $? -ne 0 ]];then
    echo "key filter error"
    exit 1
fi

INFO="[ `date -d "0 hour"  +%Y-%m-%d:%H:%M:%S` ] keyfilter done."
python notifier/dingding_notifier.py $INFO

#ftrl train
model_file="./model/lr_model_batch.dat"
cp ${model_file} ./model_bk/"lr_model_batch.dat".${file_time_flag}
./bin/ftrl ${adfea_train_instance}.kf alpha=0.01 beta=1 l1_reg=1 l2_reg=0. model_out=${model_file}.new save_aux=1 is_incre=0
if [[ $? -ne 0 ]];then
  echo "ftrl train error"
  exit 1
fi

INFO="[ `date -d "0 hour"  +%Y-%m-%d:%H:%M:%S` ] model train done."
python notifier/dingding_notifier.py $INFO

#model check
check_result=./check_result/result
./bin/model_check ${model_file}.new ${adfea_train_instance}.shuffle ${check_result}
if [[ $? -ne 0 ]];then
  python notifier/dingding_notifier.py "Error: model check error!"
  bash -x script/model_check_notifier.sh
  exit 1
else
  bash -x script/model_check_notifier.sh
fi

INFO="[ `date -d "0 hour"  +%Y-%m-%d:%H:%M:%S` ] model check done."
python notifier/dingding_notifier.py $INFO

#backup old model and swap it
mv ${root_path}/model/lr_model.dat ${root_path}/model_bk/lr_model.dat.${file_time_flag}
mv ${model_file}.new ${root_path}/model/lr_model.dat

model_online_file=${root_path}/model/lr_model_online.dat
awk -F' ' '{ if($2 != 0) print $1,$2 }' ${root_path}/model/lr_model.dat > ${model_online_file}.new
if [[ $? -ne 0 ]];then
    echo "Error: model extract the nnz weight error"
    exit 1
fi
mv ${model_online_file}.new ${model_online_file}

#scp to online
dest_path=/home/work/run_env/DEPLOY/BidMax/Bidder/data/
#BidMax-001
bidder_machine_new="192.168.18.10"
scp_model ${model_online_file} ${bidder_machine_new} ${dest_path} ${dest_model_file}
#BidMax-002
bidder_machine_new2="192.168.18.12"
scp_model ${model_online_file} ${bidder_machine_new2} ${dest_path} ${dest_model_file}
#BidMax-005
bidder_machine_new3="192.168.18.59"
scp_model ${model_online_file} ${bidder_machine_new3} ${dest_path} ${dest_model_file}
#BidMax-backup
bidder_machine_backup="192.168.18.14"
scp_model ${model_online_file} ${bidder_machine_backup} ${dest_path} ${dest_model_file}

exit 0
