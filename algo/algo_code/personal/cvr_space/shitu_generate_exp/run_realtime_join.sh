#!/bash/bin

KAFKA_DATA_PATH="/home/ad_user/personal/ling.fang/kafka_stream_data/data"
python script/join_transform.py $KAFKA_DATA_PATH/click/ $KAFKA_DATA_PATH/transform/ shitu_log/shitu.log
python script/get_creative_info.py > script/ad_info 
cat shitu_log/shitu.log | python script/mapper.py 'offline' | python script/reducer_single.py > shitu_ins/shitu_ins 
