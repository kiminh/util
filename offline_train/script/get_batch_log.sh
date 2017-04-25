#!/bin/bash

start_time_stamp="2017030100"
end_time_stamp="2017040100"
batch_shitu_log=shitu.batch
:> ${batch_shitu_log}

for file in `ls /home/work/xuyichen/shitu_log2.0/shitu_[0-9]*`;do

    file_basename=`basename ${file}`
    result=`echo "${file_basename}" | sed -n '/wap$/p'`
    if [ -n "${result}" ];then
        continue
    fi

    timeflag=${file_basename:0-10}
    
    if (( timeflag > start_time_stamp && timeflag < end_time_stamp ));then
        python get_nowap.py ${file} ${file}.tmp

        if [ $? -ne 0 ];then
            echo "get nowap fail."
            rm -rf ${file}.tmp
            break
        fi
        cat ${file}.tmp >> ${batch_shitu_log}
        rm -rf ${file}.tmp
    fi
done
