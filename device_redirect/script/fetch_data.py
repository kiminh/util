#coding:utf-8
__author__ = "fangl@bayescom.com"

"""
fetch_train_data is for fetch the given seek user 
and the audience target
"""

import sys, os, re, random
"""
def read_channel_list(filename1, filename2):
    channel_1_level = {}
    channel_2_level = {}
    with open(filename1) as f1, open(filename2) as f2:
        for raw_line in f1:
            line = raw_line.strip().split()
            channel_1_level[line[0]] = line[1].split("-")[1]
        for raw_line in f2:
            line = raw_line.strip().split()
            channel_2_level[line[0]] = ''.join(line[1].split("-")[1:])
    return channel_1_level, channel_2_level
"""
def fetch_src_files(src_file_dir):
    return [ os.path.join(src_file_dir, file) for file in os.listdir(src_file_dir) if "youku" in file ]

def read_seek_user(filename):
    deviceid_set = set()
    with open(filename) as fin:
        for raw_line in fin:
            deviceid = raw_line.strip()
            if deviceid == "":
                continue

            deviceid_set.add(deviceid)
    return deviceid_set

def filter_char(seg):
    filter_chars = [" ","】", "!", "”","[" , "]", "【", "「", "」", "》", "~", "\r", "\n", "\t", "。", "，", ",", "“", "！", "？", "！", "《", ":", "；", "：", "（", "）", "、"]
    for c in filter_chars:
        seg = seg.replace(c, "")
    return seg

def fetch_data(deviceid_set, src_files, seek_file, target_file):
    filter_chars = "[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）《》:：【】()-“]+".decode('utf-8')
    header = ["idfa", "screenhight", "screenheight", "carrier", "connectiontype", "model", 'model1', "osv", "devicetype", "ip", "title", "keywords", "channel", "cs"]

    with open(seek_file, 'w') as seek_fin, open(target_file, 'w') as target_fin:
        seek_fin.write(",".join(header) + "\n")
        target_fin.write(",".join(header) + "\n")
        for file in src_files:
            print "process {}".format(file)
            with open(file) as src_fin:
                for raw_line in src_fin:
                    line = raw_line.strip("\r\n").split("\001")
                    
                    if line[0] == "00000000-0000-0000-0000-000000000000":
                        continue

                    if len(line) == 13 and line[9] != "": 
                        if line[0] == "": 
                            continue
                        line[9] = filter_char(line[9])
                        ret = ','.join(line)
                        sp = ret.split(',')

                        if len(sp) != 14: 
                            continue
                        try:
                            l = int(sp[9].split('.')[0])
                        except:
                            continue

                        try:
                            ret = sp[10].decode("utf-8").encode("gbk")
                        except:
                            continue

                        write_str = ",".join(line) + "\n"
                        if line[0] in deviceid_set:
                            seek_fin.write(write_str)
                        else:
                            #if random.randint(1, 10) < 2:
                            target_fin.write(write_str)

if __name__ == "__main__":

    if len(sys.argv) < 5:
        print "Usage : python fetch_data.py src_file_dir in_seek_file out_seek_file target_file"
        exit()
    #channel_list1, channel_list2 = read_channel_list("./channel_list/1_channel.txt", "./channel_list/2_channel.txt")
    deviceid_set = read_seek_user(sys.argv[2])
    file_list = fetch_src_files(sys.argv[1])
    fetch_data(deviceid_set, file_list, sys.argv[3], sys.argv[4])
