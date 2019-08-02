#!/bin/bash
source ./script/functions.sh

file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
pid=${file_time_flag}
echo $file_time_flag
[[ ! -e history_adfea ]] && mkdir history_adfea
[[ ! -e model ]] && mkdir model

root_path=`pwd`
done_path=${root_path}/done_path
batch_adfea_done="${done_path}/adfea_ssp_done.last"
[[ ! -f ${batch_adfea_done} ]] && echo "adfea batch done not exist " && exit 0
source ${batch_adfea_done}

#adfea
adfea_run_temple="./conf/run_temple.conf"
adfea_run_conf="./conf/run.conf"
adfea_output=./history_adfea/conversion_log.fea
cp ${adfea_run_temple} ${adfea_run_conf}
echo "fea_conf=./conf/fea_list.conf" >> ${adfea_run_conf}
echo "train_file=${conversion_log}" >> ${adfea_run_conf}
echo "train_instance=${adfea_output}" >> ${adfea_run_conf}
./bin/adfea ${adfea_run_conf}
if [[ $? -ne 0 ]];then
    echo "adfea error"
    exit 1
fi

mv ${batch_adfea_done} ${batch_adfea_done}.bk
echo "conversion_log=${conversion_log}" >> ${batch_adfea_done}
echo "dest_model_file=${dest_model_file}" >> ${batch_adfea_done}

./bin/shuffle ${adfea_output} ${adfea_output}.shuffle 5000000
if [[ $? -ne 0 ]];then
  echo "shuffle error"
  exit 1
fi

model_file=./model/lr_cvr_model.dat
mv ${model_file} ${model_file}.bk
train_run_conf="./conf/ftrl.conf"
train_run_conf_tmp="./conf/ftrl_tmp.conf"
cp ${train_run_conf_tmp} ${train_run_conf}
echo "train_file=${adfea_output}.shuffle" >> ${train_run_conf}
echo "model_file=${model_file}" >> ${train_run_conf}
./bin/train 10000000 ${train_run_conf}
if [[ $? -ne 0 ]];then
  echo "train error!"
  exit 1
fi

dest_path=/home/work/run_env/DEPLOY/BidMax/Bidder/data/
#BidMax-001
bidder_machine_new="192.168.18.10"
scp_model ${model_file} ${bidder_machine_new} ${dest_path} ${dest_model_file}
#BidMax-002
bidder_machine_new2="192.168.18.12"
scp_model ${model_file} ${bidder_machine_new2} ${dest_path} ${dest_model_file}
#BidMax-002
bidder_machine_new3="192.168.18.59"
scp_model ${model_file} ${bidder_machine_new3} ${dest_path} ${dest_model_file}
#BidMax-backup
bidder_machine_backup="192.168.18.14"
scp_model ${model_file} ${bidder_machine_backup} ${dest_path} ${dest_model_file}

exit 0
