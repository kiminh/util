#!/bin/bash



file_time_flag=$(date -d "-2 hour"  +%Y%m%d%H)

root_path=`pwd`
done_path=${root_path}/done_path

shitu_path=/home/work/data/shitu_log


work_path="./online_auc"
[[ ! -d ${work_path} ]] && mkdir ${work_path}


auc_path="auc_report"
[[ ! -d ${auc_path} ]] && mkdir ${auc_path}


#current_day=$(date  +%Y%m%d%H)

index=5

i=2
latest_shitu="${work_path}/current_shitu"

current_time=${file_time_flag}

[[ -f ${latest_shitu} ]] &&  mv  ${latest_shitu} ${latest_shitu}.bk

while (( i < index ));do
    cat ${shitu_path}/shitu_${current_time}* >> ${latest_shitu}
    current_time=$(date -d "-${i} hour" +%Y%m%d%H)
    let i+=1
done


#############

#python script/process_ctr_tag.py ${latest_shitu} ${latest_shitu}.pctr ${latest_shitu}.valid  2
python script/process_ctr_all.py ${latest_shitu} ${latest_shitu}.pctr ${latest_shitu}.valid
if [[ $? -ne 0 ]];then
    echo "process ctr error"
    exit 1
fi
gawk -F'\001'  '{print $1}' ${latest_shitu}.valid  > ${latest_shitu}.label

paste -d' ' ${latest_shitu}.pctr ${latest_shitu}.label > ${latest_shitu}.result
if [[ $? -ne 0 ]];then
    echo "get online pctr result error"
    exit 1
fi

python bin/cal_metric_auc.py ${latest_shitu}.result > ${auc_path}/auc.${file_time_flag}
if [[ $? -ne 0 ]];then
    echo "no positive_sample!" > ${auc_path}/auc.${file_time_flag}
fi

output_file="notifier/online_auc"
echo -en "oversea_ssp_base " > ${output_file}
cat ${auc_path}/auc.${file_time_flag} >> ${output_file}


alert_info=`cat ${output_file}`
python notifier/dingding_notifier.py "${alert_info}"

exit 0
