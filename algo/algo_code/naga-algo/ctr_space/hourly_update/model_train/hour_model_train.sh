#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
VERSION=`date -d "0 days ago" +%Y%m%d%H%M%S`
if [[ $# -eq 2 ]];then
    file_time_flag=$1
    VERSION=$2
fi
file_day_flag=$(date -d "0 day" +%Y%m%d)

USER_NAME=ling.fang
HADOOP_HOME="/usr/local/hadoop-2.6.3"
root_path=`pwd`
common_file="$root_path/../../../script/tools/common.sh"
source $common_file

root_path=`pwd`
done_path=${root_path}/done_path
train_done="${done_path}/train_done.last"
[[ ! -f ${train_done} ]] && echo "batch done not exist " && exit 0
source ${train_done}

incre_path=./incre_adfea
model_bk=./model_bk/
[[ ! -d ${incre_path} ]] && mkdir ${incre_path}
[[ ! -e ${model_bk} ]] && mkdir ${model_bk}
incre_shitu=${incre_path}/incre_shitu
incre_shitu=${incre_shitu}.${file_time_flag}
:>${incre_shitu}

last_end_time_stamp=${end_time_stamp}
new_end_time_stamp=${end_time_stamp}

#rebatch
is_batch_model_ready=0
if [[  -e $batch_model_path/model/lr_model.dat.${file_day_flag} ]];then
    is_batch_model_ready=1
fi

if [[ $is_batch_model_ready -eq 1 ]];then
    last_end_time_stamp=${file_day_flag}00
    new_end_time_stamp=${file_day_flag}00
fi

for file in `ls $root_path/../shitu_generate2/shitu/shitu_[0-9]* | tac`;do
    echo $file
    file_basename=`basename $file` 
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

mv ${train_done} ${train_done}.bk
echo "end_time_stamp=${new_end_time_stamp}" > ${train_done}
echo "data_path=${data_path}" >> ${train_done}
echo "batch_model_path=${batch_model_path}" >> ${train_done}

model_file="./model/lr_model.dat"
#rebatch
if [[ $is_batch_model_ready -eq 1 ]];then    
    model_file=$batch_model_path/model/lr_model.dat.${file_day_flag}
fi
new_model_file="./model/lr_model.dat.new"

./ftrl/bin/ftrl ${incre_shitu} alpha=0.02 beta=1 l1_reg=1 l2_reg=0. \
           model_out=${new_model_file} save_aux=1 is_incre=1 model_in=${model_file}
if [[ $? -ne 0 ]];then
  echo "ftrl train error"
  exit 1
fi

if [[ $is_batch_model_ready -eq 0 ]];then
    mv ${model_file} ./model_bk/"lr_model.dat".$VERSION
    mv ${new_model_file} ${model_file}
else
    mv ${new_model_file} ./model/lr_model.dat
fi

#########补最近一小时的tmp日志############
data_path_tmp=$root_path/../shitu_generate2/shitu_tmp/
tmp_shitu="${data_path_tmp}/shitu_${file_time_flag}"
tmp_shitu_online=$root_path/incre_adfea/incre_tmp
:>$tmp_shitu_online
[[ -e $tmp_shitu ]] && cp $tmp_shitu $tmp_shitu_online

model_file="./model/lr_model.dat"
model_online_file="./model/lr_model_online.dat"
mv $model_online_file ./model_bk/lr_model_online.dat.$VERSION

./ftrl/bin/ftrl ${tmp_shitu_online} alpha=0.02 beta=1 l1_reg=1 l2_reg=0. \
           model_in=${model_file} model_out=${model_online_file} save_aux=1 is_incre=1
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model" "model push online $VERSION error!" 
    exit 1
fi

if [[ $is_batch_model_ready -eq 1 ]];then    
    mv $batch_model_path/model/lr_model.dat.${file_day_flag} $batch_model_path/model_bk/
fi

python script/model_push_util.py $VERSION conf/model_push.conf
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model" "model push online $VERSION error!"   
    exit 1
fi
alarm "Nage DSP Hour Model" "[$VERSION] ftrl ctr model train success! $bias_res"
exit 0
