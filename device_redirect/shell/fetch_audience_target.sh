#!/bin/bash
if [[ $# -lt 2 ]];then
    echo "fetch_data seek_user_data need_num_deviceid"
    exit 1
fi
echo $1, $2

python script/fetch_data.py /home/work/disk/shuttle_data/youku $1 seekuser_package/out_seek.log target_package/out_target.log
#head -1000000 target_package/out_target.log > target_package/tmp
for((i=0;i<100;i++));do
    echo "idfa,screenhight,screenheight,carrier,connectiontype,model,model1,osv,devicetype,ip,title,keywords,channel,cs" > target_package/tmp
    awk -F',' 'BEGIN{srand(systime())};{if(rand() * 100 < 5) print $0}' target_package/out_target.log >> target_package/tmp
    mv result_package/idfa.txt result_package/idfa.txt.tmp
    python script/train.py seekuser_package/out_seek.log target_package/tmp result_package/idfa.txt
    if [[ $? -ne 0 ]];then
        exit 1
    fi
    cat result_package/idfa.txt.tmp >> result_package/idfa.txt
    sort result_package/idfa.txt | uniq > result_package/idfa
    mv result_package/idfa result_package/idfa.txt

    num=`sed -n "$=" result_package/idfa.txt`
    if [[ $num -gt $2 ]];then
        echo "Get Target Audience Finished..."
        exit 0
    fi
done
