#!/bin/bash
HADOOP_BIN="/usr/local/hadoop-2.6.3/bin/hadoop"
ROOT_DIR=`pwd`

function mylog() {
    now=$(date "+%Y%m%d-%H:%M:%S")
    echo "["$now"] "$1
}

function clear_hadoop_data() {
    [ $# -lt 2 ] && return;
    path=$1
    windows=$2
    start_dt=`date -d "1 days ago" +"%Y%m%d"`
    [ $# -gt 2 ] && start_dt=$3
    start_dt=`date -d "$windows days ago $start_dt" +"%Y%m%d"`
    for idx in `seq 0 7`;do
        date=`date -d "$idx days ago $start_dt" +"%Y%m%d"`
        $HADOOP_BIN fs -test -e $path/$date && $HADOOP_BIN fs -rm -r $path/$date
    done
    return 0
}


function hadoop_check_ready() {
   $HADOOP_BIN fs -test -e $file && ready=1 || \
   { ready=0;}
}

function local_check_ready() {
    [ -f $file ] && ready=1 || \
    { ready=0;}
}

function redis_check_ready() {
    res_code=`python /home/ling.fang/script/tools/get_flag.py $file`
    [[  "X$res_code" == X"True" ]] && ready=1 || \
    { ready=0;}
}

function loop_check() {
    file=$1
    ready=0
    #check unit is 10 minutes
    check_time=$2
    mode=$3
    echo "input params:$*"
    if [ $# -ne 3 ]; then
        echo "usage: sh $0 check_file check_time mode"
        echo "mode=1 check hadoop file"
        echo "mode=0 check local file"
        echo "mode=2 check redis key"
        return 1
    fi
    while [ $check_time -gt 0 ]; do
        if [ $mode -eq 1 ];then
            hadoop_check_ready
        elif [ $mode -eq 2 ];then
            redis_check_ready
        else
            local_check_ready
        fi

        if [ $ready -eq 1 ]; then
            mylog "$file is ready!"
            return 0
        else
            mylog "$file is not ready, check [$check_time]"
            ((check_time=check_time-1))
            sleep 600 
        fi
    done

    [ $check_time -eq 0 ] && mylog "$file is not ready, exit 1" && exit 1
}

function job_run(){
    cmd=$1
    retry_cnt=$2

    count=0
    flag=0
    while [ 0 -eq 0 ]
    do
        bash -x $cmd    
        if [ $? -eq 0 ];then
            echo "--------------- job complete ---------------"
            return 0;
        else
            count=$[${count}+1]
            if [ ${count} -eq $retry_cnt ];then
                mylog "$cmd run fail, exit 1" && return 1
            fi
            echo "...............retry in 2 seconds .........."
            sleep 2
        fi  
    done
    return 0
}

#clear_hadoop_data /user/john.zhu/ocpc/click_join_trans 3 20180915
#loop_check flag_ods_usage_data_h_usage_ad_dsp_ed_abroad##20190821 140 2
