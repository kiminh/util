#!/bash/bin

work_path=/home/work/jax.wang/personal/ling.fang/
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file
DATE=`date -d "1 days ago" "+%Y%m%d"`
echo $DATE


#cd $work_path/ctr_space_exp/ed_join_click_spark/ && bash -x shell/ed_join_click.sh $DATE
#if [[ $? -ne 0 ]];then
#    alarm "Nage DSP daily ed_join_click $DATE ERROR!"
#    exit 1
#fi

cd $work_path/ctr_space_exp/daily_update/shitu_generate && bash -x run_shitu.sh  
if [[ $? -ne 0 ]];then
    alarm "Nage DSP daily run shitu $DATE ERROR!"
    exit 1
fi

cd $work_path/ctr_space_exp/daily_update/model_train/ &&  bash -x model_train_new.sh
if [[ $? -ne 0 ]];then
    alarm "Nage DSP daily model train $DATE ERROR!"
    exit 1
fi
alarm "Nage DSP daily model train $DATE SUCCESS!"

mv $work_path/ctr_space_exp/jobs/daily_task.txt $work_path/ctr_space_exp/jobs/log/daily_task_$DATE.txt 
