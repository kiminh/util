#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
bidder_machine=10.175.245.175
remote_machine=10.175.247.236

dest_file_path=/home/work/chenjing/data
con_file_path=/home/work/chenjing/conversion_data
out_put_path=/home/work/chenjing/shitu_log
adfea_path=/home/work/chenjing/bayesalgo/adfea/
fea_path=/home/work/chenjing/bayesalgo/
model_path=/home/work/chenjing/bayesalgo/ftrl/
current_path=$(cd "$(dirname "$0")"; pwd)
done_file_path=/home/work/chenjing/done_path

model_done_tag=$done_file_path/done.model.$file_time_flag
shitu_done_tag=$done_file_path/done.shitu.$file_time_flag

if [ ! -d $done_file_path ]; then
	mkdir $done_file_path
fi
#check model done
if [ -f $model_done_tag ] ; then
	exit 0
fi

echo $file_time_flag
#check shitu done tag
if [ -f $shitu_done_tag ] ; then
	echo $shitu_done_tag
	touch $model_done_tag
	cd $adfea_path 
	./bin/adfea ./conf/run.conf
	
	echo "Begin shuffle..."
	#for shuffle 路径在run.conf
	train_instance=/home/work/chenjing/bayesalgo/ftrl/data/train
	temp_instance=/home/work/chenjing/bayesalgo/ftrl/data/train_1
	mv $train_instance $temp_instance
	python $current_path/shuffle.py $temp_instance $train_instance 0 20000000
	echo "End shuffle"
	#end shuffle
	
	cd $model_path
	./bin/train 5000000 ./conf/ftrl.conf
	
	touch $model_done_tag

	scp /home/work/chenjing/model/model.dat work@$bidder_machine:/home/work/run_env/bayes_rtbkit/data/lr_model.dat.1
	ssh work@$bidder_machine "cd /home/work/run_env/bayes_rtbkit/data/ && mv lr_model.dat lr_model.dat.bak  && mv lr_model.dat.1 lr_model.dat" 
fi
