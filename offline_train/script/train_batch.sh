#!/bin/bash

need_get_log=0
need_adfea=1
need_get_train_and_test=1
need_shuffle=0
need_train=0

#get shitu log
start_time_stamp="2017040100"
end_time_stamp="2017040800"
batch_shitu_log="./shitu_log/shitu.log"

adfea_conf=./conf/run.conf
ftrl_conf=./conf/ftrl.conf
adfea_conf_temp=./conf/run_temp.conf
ftrl_conf_temp=./conf/ftrl_temp.conf

if [ ${need_get_log} -eq 1 ];then

    :> ${batch_shitu_log}

    for file in `ls /home/work/xuyichen/shitu_log2.0/shitu_[0-9]*`;do

        file_basename=`basename ${file}`
        result=`echo "${file_basename}" | sed -n '/wap$/p'`
        if [ -n "${result}" ];then
            continue
        fi

        timeflag=${file_basename:0-10}
    
        if (( timeflag > start_time_stamp && timeflag < end_time_stamp ));then
            python ./script/get_nowap.py ${file} ${file}.tmp

            if [ $? -ne 0 ];then
                echo "get nowap fail."
                rm -rf ${file}.tmp
                break
            fi
            cat ${file}.tmp >> ${batch_shitu_log}
            rm -rf ${file}.tmp
        fi
    done
fi

#adfea
adfea_fea_conf="./conf/fea_list_new.conf"
adfea_train_file=${batch_shitu_log}
adfea_train_instance="./shitu_ins/shitu.ins"

if [ ${need_adfea} -eq 1 ];then

    cp ${adfea_conf_temp} ${adfea_conf}

    echo "fea_conf=${adfea_fea_conf}" >> ${adfea_conf}
    echo "train_file=${adfea_train_file}" >> ${adfea_conf}
    echo "train_instance=${adfea_train_instance}" >> ${adfea_conf}

    ./bin/adfea ${adfea_conf}
    if [ $? -ne 0 ];then
        echo "adfea fail"
        exit 1
    fi

fi
    #get train shitu instance
    ftrl_train_instance="./shitu_ins/shitu.train"
    ftrl_valid_instance="./shitu_ins/shitu.test"

if [ ${need_get_train_and_test} -eq 1 ];then

    ins_num=`sed -n $= ${adfea_train_instance}`
    train_ins_num=$(echo " 5 * ${ins_num} / 10" | bc)
    test_ins_num=$(echo " ${ins_num} - ${train_ins_num} " | bc)

    head -${train_ins_num} ${adfea_train_instance} > ${ftrl_train_instance}

    #get test shitu instance
    tac ${adfea_train_instance} | head -${test_ins_num} > ${ftrl_valid_instance}
fi

ftrl_train_instance_shuffle=${ftrl_train_instance}

if [ ${need_shuffle} -eq 1 ];then
    ftrl_train_instance_shuffle=${ftrl_train_instance}.shuffle
    ./bin/shuffle ${ftrl_train_instance} ${ftrl_train_instance_shuffle} 5000000
fi

cp ${ftrl_conf_temp}  ${ftrl_conf}
echo "model_file=model.dat" >> ${ftrl_conf}
echo "predict_out=result.dat" >> ${ftrl_conf}
echo "train_file=${ftrl_train_instance_shuffle}" >> ${ftrl_conf}
echo "valid_file=${ftrl_valid_instance}" >> ${ftrl_conf}

if [ ${need_train} -eq 1 ];then

    #train
    ./bin/train 30000000 ${ftrl_conf}

    if [ $? -ne 0 ];then
        echo "train error"
        exit 1
    fi

fi
#predict
./bin/predict ${ftrl_conf}

#cal metric
python ./bin/cal_metric.py result.dat
