#!/bash/bin

flag=`ps -ef | grep get_log.py | grep -v grep`
if [[ -z $flag ]];then
    nohup python get_log.py &
fi
