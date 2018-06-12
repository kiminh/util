#!/bin/bash
shuttle001_machine=192.168.149.68
remote_log_path=/home/work/log/shuttle/adxreq/youku

scp work@$shuttle001_machine:$remote_log_path/* ./data/youku

exit 0
