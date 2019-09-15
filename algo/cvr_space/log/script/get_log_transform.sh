#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
echo $file_time_flag

done_file_path="/home/ling.fang/cvr_space/log/done_path"
done_file=${done_file_path}/transform.done
source ${done_file}

incre_click_log=click_tmp_file_${file_time_flag}
:>${incre_click_log}
new_end_time_stamp=${end_timestamp}
last_end_time_stamp=${end_timestamp}
for file in `ls /home/ling.fang/ctr_space/hourly_update/log/click/click\.[0-9]* | tac`;do
    basename=`basename ${file}`
    timeflag=${basename:6:10}
    if (( timeflag > last_end_time_stamp && timeflag <= file_time_flag ));then
        cat $file | python script/filter_click.py >> ${incre_click_log}
        if (( timeflag > new_end_time_stamp ));then
            new_end_time_stamp=${timeflag}
        fi  
    fi  
done

cat /home/ling.fang/ctr_space/hourly_update/log/transform/* > ${transform_log}
cat ${incre_click_log} >> ${click_log}
cp ${shitu_log} ${shitu_log}.bk

python script/join_transform.py ${click_log} ${transform_log} ${shitu_log}
if [[ $? -ne 0 ]];then
    echo "join transform error!"
    exit 1
fi

cp ${done_file} ${done_file}.bk
echo "end_timestamp=${new_end_time_stamp}" > ${done_file}
echo "click_log=${click_log}" >> ${done_file}
echo "transform_log=${transform_log}" >> ${done_file}
echo "shitu_log=${shitu_log}" >> ${done_file}

rm -rf ${incre_click_log}
exit 0
