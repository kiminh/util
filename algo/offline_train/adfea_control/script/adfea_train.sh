#!/bin/bash



bidder_machine="192.168.13.177"

file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)

pid=${file_time_flag}

echo $file_time_flag

root_path=`pwd`
done_path=${root_path}/done_path


exchange="dmssp"

batch_adfea_done="${done_path}/adfea_${exchange}_done.last"



work_lock="${done_path}/adfea_${exchange}_incre.lock"
[[  -f ${work_lock} ]] && echo "adfea is running" && exit 0

[[ ! -f ${batch_adfea_done} ]] && echo "adfea batch done not exist " && exit 0


source ${batch_adfea_done}

shitu_path=/home/work/xuyichen/shitu_log2.0


incre_path=./incre_adfea_${exchange}
[[ ! -d ${incre_path} ]] && mkdir ${incre_path}

feature_output=${incre_path}

[[ ! -d ${feature_output} ]] && mkdir ${feature_output}

last_end_time_stamp=${end_time_stamp}

incre_shitu=${incre_path}/incre_shitu_log
:>${incre_shitu}

touch ${work_lock}

incre_shitu=${incre_shitu}.${file_time_flag}

new_end_time_stamp=${end_time_stamp}

:>${incre_shitu}

new_end_time_stamp=${end_time_stamp}
for file in `ls /home/work/xuyichen/shitu_log2.0/shitu_[0-9]* | tac`;do
    timeflag=${file:0-10}
    if (( timeflag > last_end_time_stamp ));then
        gawk -F'\001'  '$8=="dmssp"' ${file}  >> ${incre_shitu}
        if (( timeflag > new_end_time_stamp ));then
            new_end_time_stamp=${timeflag}
        fi
    else
        break
    fi
done



adfea_run_temple="./conf/run_temple.conf"
adfea_run_conf="./conf/run_${exchange}.conf"

adfea_run_exchange_conf="./conf/run_${exchange}.conf"

adfea_exchange_output=${incre_shitu}.fea
cp ${adfea_run_temple} ${adfea_run_exchange_conf}
echo "fea_conf=./conf/fea_list_dmssp.conf" >> ${adfea_run_exchange_conf}
echo "train_file=${incre_shitu}" >> ${adfea_run_exchange_conf}
echo "train_instance=${adfea_exchange_output}" >> ${adfea_run_exchange_conf}

./bin/adfea ${adfea_run_exchange_conf}
if [[ $? -ne 0 ]];then
    echo "adfea error"
    exit 1
fi

[[ -f ${adfea_result_exchange} ]] && cp ${adfea_result_exchange}  ${adfea_result_exchange}.bk
cat ${adfea_exchange_output} >> ${adfea_result_exchange}


[[ -f ${shitu_log} ]] && cp ${shitu_log} ${shitu_log}.bk
cat ${incre_shitu}  >> ${shitu_log}


mv ${batch_adfea_done} ${batch_adfea_done}.bk
echo "end_time_stamp=${new_end_time_stamp}" > ${batch_adfea_done}
echo "adfea_result_exchange=${adfea_result_exchange}" >> ${batch_adfea_done}
#echo "adfea_result_nocombine=${adfea_result_nocombine}" >> ${batch_adfea_done}
echo "shitu_log=${shitu_log}" >> ${batch_adfea_done}

########################################################################################


train_run_temple="./conf/ftrl_temple_${exchange}.conf"
train_run_conf="./conf/ftrl_exchange_${exchange}.conf"
cp ${train_run_temple} ${train_run_conf}

./bin/shuffle ${adfea_result_exchange} ${adfea_result_exchange}.shuffle 10000000
if [[ $? -ne 0 ]];then
    echo "shuffle error"
    exit 1
fi
#model_file="./model/lr_model_exchange.dat"

model_file="./model/lr_model_${exchange}.dat"
echo "train_file=${adfea_result_exchange}.shuffle" >> ${train_run_conf}
echo "model_file=${model_file}" >> ${train_run_conf}

./bin/train 5000000 ${train_run_conf}
if [[ $? -ne 0 ]];then
    echo "train error"
    exit 1
fi


bidder_machine_new="192.168.13.177"
scp ${model_file} ${bidder_machine_new}:/home/work/run_env/DEPLOY/BidMax/Bidder/data/lr_model_dmssp.dat.new
ssh work@${bidder_machine_new} "cd /home/work/run_env/DEPLOY/BidMax/Bidder/data/;
    [[ -e lr_model_dmssp.dat ]] && mv lr_model_dmssp.dat lr_model_dmssp.dat.bk;
    mv lr_model_dmssp.dat.new lr_model_dmssp.dat" 


rm -rf ${work_lock}
exit 0
