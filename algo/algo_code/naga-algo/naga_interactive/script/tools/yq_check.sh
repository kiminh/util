#!/bin/sh

hadoop=/usr/local/hadoop-2.6.3/bin/hadoop

function hadoop_check_ready() {
   $hadoop fs -test -e $file && ready=1 || \
   { ready=0;}
}

function local_check_ready() { 
    [ -f "$file" ] && ready=1 || \
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
        return 1
    fi
    while [ $check_time -gt 0 ]; do
        if [ $mode -eq 1 ];then
            hadoop_check_ready
        else
            local_check_ready
        fi

        if [ $ready -eq 1 ]; then
            echo "$file is ready!"
            return 0
        else
            echo "$file is not ready, check [$check_time]"
            ((check_time=check_time-1))
            sleep 600 
        fi
    done

    [ $check_time -eq 0 ] && echo "$ is not ready, exit 1" && return 1
}
