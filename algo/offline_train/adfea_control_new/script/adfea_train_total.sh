#!/bin/bash
source ./script/functions.sh
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
pid=${file_time_flag}
echo $file_time_flag

#check the rebatch process
batch_process=`ps -ef | grep "batch_train_check.sh" | grep -v grep`
if [ -n "$batch_process" ];then
  echo "batch training now..."
  exit 0
fi

root_path=`pwd`
done_path=${root_path}/done_path
batch_adfea_done="${done_path}/adfea_ssp_done.last"
[[ ! -f ${batch_adfea_done} ]] && echo "adfea batch done not exist " && exit 0
source ${batch_adfea_done}

incre_path=./incre_adfea
[[ ! -d ${incre_path} ]] && mkdir ${incre_path}
incre_shitu=${incre_path}/incre_shitu
incre_shitu=${incre_shitu}.${file_time_flag}
:>${incre_shitu}

last_end_time_stamp=${end_time_stamp}
new_end_time_stamp=${end_time_stamp}
for file in `ls $data_path/shitu_[0-9]* | tac`;do
 
    file_basename=`basename ${file}`
    result=`echo "${file_basename}" | sed -n '/wap$/p'`
    if [ -n "${result}" ];then
        continue
    fi
    timeflag=${file_basename:0-10}

    if (( timeflag > last_end_time_stamp ));then

        cat ${file} >> ${incre_shitu}
        if (( timeflag > new_end_time_stamp ));then
            new_end_time_stamp=${timeflag}
        fi
    else
        break
    fi
done

#adfea
adfea_run_temple="./conf/run_temple.conf"
adfea_run_conf="./conf/run.conf"
adfea_output=${incre_shitu}.fea
cp ${adfea_run_temple} ${adfea_run_conf}
echo "fea_conf=./conf/fea_list.conf" >> ${adfea_run_conf}
echo "train_file=${incre_shitu}" >> ${adfea_run_conf}
echo "train_instance=${adfea_output}" >> ${adfea_run_conf}
./bin/adfea ${adfea_run_conf}
if [[ $? -ne 0 ]];then
    echo "adfea error"
    exit 1
fi

mv ${batch_adfea_done} ${batch_adfea_done}.bk
echo "end_time_stamp=${new_end_time_stamp}" > ${batch_adfea_done}
echo "dest_model_file=${dest_model_file}" >> ${batch_adfea_done}

./bin/shuffle ${adfea_output} ${adfea_output}.shuffle 5000000
if [[ $? -ne 0 ]];then
    echo "shuffle error"
    cp ${adfea_output} ${adfea_output}.shuffle
fi

#ftrl train
model_file="./model/lr_model.dat"
./bin/ftrl ${adfea_output}.shuffle alpha=0.01 beta=1 l1_reg=1 l2_reg=0. \
           model_in=${model_file} model_out=${model_file}.new save_aux=1 is_incre=1
if [[ $? -ne 0 ]];then
  echo "ftrl train error"
  exit 1
fi

#model check
check_result=./check_result/result
./bin/model_check ${model_file}.new ${adfea_output}.shuffle ${check_result}
if [[ $? -ne 0 ]];then
  python notifier/dingding_notifier.py "Error: model update error!"
  bash -x script/model_check_notifiter.sh
  exit 1
else
  bash -x script/model_check_notifiter.sh
fi
mv ${model_file} ./model_bk/"lr_model.dat".${file_time_flag}
mv ${model_file}.new ${model_file}

#########补最近一小时的tmp日志############
tmp_shitu="${data_path_tmp}/shitu_${file_time_flag}"
adfea_run_temple="./conf/run_temple.conf"
adfea_run_tmp_conf="./conf/run_online_tmp.conf"

tmp_shitu_output=${incre_path}/incre_shitu_tmp
tmp_shitu_adfea=${tmp_shitu_output}.fea

cp ${adfea_run_temple} ${adfea_run_tmp_conf}
echo "fea_conf=./conf/fea_list.conf" >> ${adfea_run_tmp_conf}
echo "train_file=${tmp_shitu}" >> ${adfea_run_tmp_conf}
echo "train_instance=${tmp_shitu_adfea}" >> ${adfea_run_tmp_conf}
./bin/adfea ${adfea_run_tmp_conf}
if [[ $? -ne 0 ]];then
  echo "adfea error"
fi

./bin/shuffle ${tmp_shitu_adfea} ${tmp_shitu_output}.shuffle 1000000
if [[ $? -ne 0 ]];then
    echo "shuffle error"
    cp ${tmp_shitu_adfea} ${tmp_shitu_output}.shuffle
fi

model_online_file="./model/lr_model_online.dat"
./bin/ftrl ${tmp_shitu_output}.shuffle alpha=0.01 beta=1 l1_reg=1 l2_reg=0. \
           model_in=${model_file} model_out=${model_online_file}.new save_aux=0 is_incre=1
if [[ $? -ne 0 ]];then
  echo "ftrl train error!!"
  exit 1
fi
mv ${model_online_file}.new ${model_online_file}

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
