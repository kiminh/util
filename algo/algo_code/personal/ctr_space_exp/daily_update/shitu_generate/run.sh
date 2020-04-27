#!/bash/bin
date=`date +%Y%m%d`
for i in `seq 1 7`;do
    day=`date -d " $i days ago $date" +%Y%m%d`
    bash -x run_shitu.sh $day
done
