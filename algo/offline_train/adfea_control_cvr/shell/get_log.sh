#!/bash/bin

for file in `ls /home/work/xuyichen/shitu_log2.0/shitu_2018030*`;do

  basefile=$(basename $file)
  time=${basefile:6:10}
  echo $time
  awk -F'\001' '{ if($1 == 1) print $0 }' $file | grep -i "is_track_active\":\"1" > /home/work/data/click_data/click_$time
done
