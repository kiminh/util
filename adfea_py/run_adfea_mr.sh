#!/bash/bin
HADOOP_HOME="/usr/local/hadoop-2.6.3"
INPUTDIR="/user/ling.fang/ctr_space/shitu_log/20190814/json/*"
OUTPUTDIR="/user/ling.fang/ctr_space/shitu_log_adfea/20190814/json/*"
OWNER_INFO=ling.fang
TASK_NAME=adfea

ROOT_PATH=`pwd`
FILES=""
for file in `ls src`;do
    FILES+=$ROOT_PATH/src/$file","
done

for file in `ls conf`;do
    FILES+=$ROOT_PATH/conf/$file","
done
echo $FILES

function hadoop_run() {
    $HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR
    $HADOOP_HOME/bin/hadoop jar /usr/local/hadoop-2.6.3/share/hadoop/tools/lib/hadoop-streaming-2.6.3.jar \
        -D mapreduce.reduce.memory.mb=6144 \
        -D mapreduce.reduce.java.opts=-Xmx3276m \
        -Dmapred.map.tasks.speculative.execution=false \
        -Dmapred.reduce.tasks.speculative.execution=false \
        -Dstream.non.zero.exit.is.failure=false \
        -Dmapred.job.priority=HIGH \
        -Dmapreduce.job.queuename=ad \
        -Dmapred.map.tasks=500 \
        -Dmapred.reduce.tasks=50 \
        -Dmapred.job.map.capacity=500 \
        -Dmapred.job.reduce.capacity=50 \
        -Dmapred.job.name=${OWNER_INFO}":"${TASK_NAME} \
        -files $FILES \
        -mapper 'python adfea_mapper.py run.conf ' \
        -reducer 'python adfea_reducer.py ' \
        -input $INPUTDIR \
        -output $OUTPUTDIR
}

hadoop_run
