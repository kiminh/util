import time
import meepo
import sys

remote_hosts = []

with open("./conf/model_push.conf") as f_in:
    for raw_line in f_in:
        line = raw_line.strip("\n\r")
        if line == "" or line[0] == '#': 
            continue
        
        key, value = line.split("=")
        key_strip = key.strip("\t ")
        value_strip = value.strip("\t ")
        
        if key_strip == "remote_addr":
            remote_addr = value_strip
        else:
            continue
    print value_strip
    if remote_addr is not None:
        machine_list = remote_addr.strip("\n\r ").split(",")
        for ml in machine_list:
            ml_sp = ml.strip().split(":")
            if len(ml_sp) == 2:
                remote_hosts.append({"host": ml_sp[0], "port": int(ml_sp[1])})
            else:
                print("remote address format error!")
                exit(1)
    else:
        print("must set the remote address!!")
        exit(1)
print remote_hosts
local_addr = {"host": "192.168.0.30", "port": 3123}

def init_meepo_writer():
    try:
        meepo_writer = meepo.Writer(remote_hosts, 
                                    local_addr)
        meepo_writer.set_send_buffer(4 * 1024)
        meepo_writer.set_send_timeout(20000)
        if meepo_writer.initialize():
            return meepo_writer
        else:
            return None
    except Exception as e:
        print("meepo write create error! [%s]" % e)
        exit(1)

meepo_writer = init_meepo_writer()
ver="20200117200736"
meepo_writer.remove_snapshot("ad", "ctr_ftrl_exp", int(ver))
exit()
for ver in open(sys.argv[1]):
    print ver
    if len(ver) > 20:
        continue
    meepo_writer.remove_snapshot("ad", "ctr_ftrl_exp", int(ver))
