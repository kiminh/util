#!/bash/bin

for i in `seq 1 7`;do
    day=`date -d "$i days ago" +%Y%m%d`
    bash -x run_shitu_daily.sh $day
done
