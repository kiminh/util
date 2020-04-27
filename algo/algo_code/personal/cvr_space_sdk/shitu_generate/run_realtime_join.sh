#!/bash/bin

python script/join_transform.py ../calibrate/kafka/online_log/click/ ../calibrate/kafka/online_log/transform/ shitu_log/shitu.log
cat shitu_log/shitu.log | python script/mapper_new.py 'offline' | python script/reducer_single.py > shitu_ins/shitu_ins 
