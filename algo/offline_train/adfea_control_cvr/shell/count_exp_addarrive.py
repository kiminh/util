#!/usr/bin/env python 
import sys
import math
import operator
import json

if len(sys.argv) < 2:
    print help
'''pattern=label_index,price,BID,time,auction_id,auction_id,time,exchange,provider,userAgentIP,Medium_id,Medium_Name,Medium_Domain,Medium_Cat,didsha1,didmd5,dpidsha1,dpidmd5,device_carrier,device_make,device_model,device_os,device_osv,device_type,connectiontype,language,location,url,ipAddress,userAgent,hour,weekday,device_string,bid_price,account,bid_agent,meta,creativeID,creativeName
'''
coutn_file = open(sys.argv[1], 'r')

total = 0
pos_num = 0
neg_num = 0
total_price = 0
total_action = 0
aduser_dict = {}
creative_dict = {}
exchange_dict = {}
traffic_dict={}
bundle_dict = {}
ad_dict = {}

total_dict = {}

for line in coutn_file:
    fleds = line.strip().split("\001")

    if len(fleds) <49:
        continue
    price = fleds[1]
    if price == "999":
        continue
    if price.find("CNY")!= -1:
        price_num = int(price.split('CNY')[0])
    else:
        price_num = int(price.split('USD')[0])
    total_price = total_price + price_num

    label = int(fleds[0])
    if label != 1:
        label = 0
    if label == 0:
        neg_num = neg_num + 1
    else:
        pos_num = pos_num + 1
    load = 0 
    action = 0
    total_action += action
    try:
        exp_dict = json.loads(fleds[38])
    except:
        print "format[%s] not json[%s]"%(fleds[38],fleds[4])
        continue

    if not isinstance(exp_dict, dict):
        print "format error[%s]!"%(fleds[38])
        continue
    try:
      pcvr = float(exp_dict["pcvr"])
    except:
      pcvr = 0.0

    for key,value in exp_dict.items():

        if key.find("_id")  == -1:
            continue
        key_str = "%s_%s"%(key,value)
        
        if key_str not in total_dict:
          total_dict[key_str] = {"clk":0, "price":0, "action":0, "pcvr":0 }

        total_dict[key_str]["price"] += price_num
        total_dict[key_str]["clk"] += 1
        total_dict[key_str]["action"] += label
        total_dict[key_str]["pcvr"] += pcvr


    creative=fleds[39]
    traffic = fleds[12]
    ad = fleds[42]

    if ad not in aduser_dict:
      aduser_dict[ad] = {"clk":0, "cost":0, "action":0}
    aduser_dict[ad]["clk"] += 1
    aduser_dict[ad]["action"] += label
    aduser_dict[ad]["cost"] += price_num
   
    if creative not in creative_dict:
      creative_dict[creative] = {"clk":0, "cost":0, "action":0}
    creative_dict[creative]["clk"] += 1
    creative_dict[creative]["action"] += label
    creative_dict[creative]["cost"] += price_num
    
    if traffic not in traffic_dict:
      traffic_dict[traffic] = {"clk":0, "cost":0, "action":0}
    traffic_dict[traffic]["clk"] += 1
    traffic_dict[traffic]["cost"] += price_num
    traffic_dict[traffic]["action"] += action

coutn_file.close()

for k,sub_dict in sorted(traffic_dict.items(),cmp=lambda x,y : cmp(x[0], y[0]),reverse=False):
    click = float(sub_dict["clk"])
    action = float(sub_dict["action"])
    cvr = click/(action+0.000001)
    cost = float(sub_dict["cost"])
    cpc=cost/1000000.0 / (action+0.000001)
    cpm=cost/ 1000.0 / click
    cost=cost/1000000.0
    #load =  float(sub_dict["load"])
    #action =  float(sub_dict["action"])
    #load_r = 0.0
    #action_r = 0.0
    #if click > 0:
    #    load_r = load/float(click)
    #    action_r = action/float(click)
   # print("Traffic %s SHOW:%10d, CLICK:%7d CTR:%10f CPM:%10f CPC:%10f LOAD:%10d LOADR:%10f ACTION:%10d CVR:%f" % (k,show,click,ctr,cpm,cpc,load,load_r,action,action_r))


for k,sub_dict in sorted(exchange_dict.items(),cmp=lambda x,y : cmp(x[0], y[0]),reverse=False):
    action = float(sub_dict["clk"])
    click = float(sub_dict["clk"])
    ctr = click/action
    #cost = float(sub_dict["cost"])
    #cpc=cost/1000000.0 / (click+0.0000001)
    #cpm=cost/ 1000.0 / show
    #cost=cost/1000000.0
    #load =  float(sub_dict["load"])
    #action =  float(sub_dict["action"])
    #print("exchange %20s SHOW:%10d, CLICK:%7d ,CTR:%10f" % (k,show,click,ctr))


for k,sub_dict in sorted(aduser_dict.items(),cmp=lambda x,y : cmp(x[0], y[0]),reverse=False):
    action = float(sub_dict["action"])
    click = float(sub_dict["clk"])
    cvr = click/(action + 0.0000001)
    cost = float(sub_dict["cost"])
    cpm=cost/ 1000.0 / click

    #print("aduser %s CLICK:%7d ACTION:%7d COST:%10f CVR:%10f CPM:%10f" % (k, click, action, cost, cvr, cpm))

for k,sub_dict in sorted(creative_dict.items(),cmp=lambda x,y : cmp(x[0], y[0]),reverse=False):
    action = float(sub_dict["action"])
    click = float(sub_dict["clk"])
    cvr = click/(action + 0.000001)
    cost = float(sub_dict["cost"])
    cpm=cost/ 1000.0 / click
    
    #print("creative %s CLICK:%7d ACTION:%7d COST:%10f CVR:%10f CPM:%10f" % (k, click, action, cost, cvr, cpm))


for k,sub_dict in sorted(bundle_dict.items(),cmp=lambda x,y : cmp(x[0], y[0]),reverse=False):
    action = float(sub_dict["action"])
    click = float(sub_dict["clk"])
    ctr = click/(action + 0.000001)
    cost = float(sub_dict["cost"])
    cpc=cost/1000000.0 / (action+0.000001)
    cpm=cost/ 1000.0 / click
    cost=cost/1000000.0
    load =  float(sub_dict["load"])
    action =  float(sub_dict["action"])
    load_r = 0.0
    action_r = 0.0
   # if click > 0:
    #    load_r = load/float(click)
     #   action_r = action/float(click)
   # cpa = cost/(action+0.0000000001)
 #   print("bundle %s SHOW:%10d, CLICK:%7d CTR:%10f CPM:%10f CPC:%10f LOAD:%10d LOADR:%10f ACTION:%10d CVR:%f CPA:%10f" % (k,show,click,ctr,cpm,cpc,load,load_r,action,action_r,cpa))

total = neg_num + pos_num
pos_rate = (0.0 + pos_num) / total
cpm = total_price / (total + 0.0) / 1000
cost_all = total_price / 1000000.0
cpc = total_price / 1000000.0 / (pos_num+0.1) 
#cpa = cost_all / (total_action+0.00001)
#print("Total   SHOW:%10d, CLICK:%7d COST:%10f CPM:%10f CTR:%10f CPC:%10f CPA:%10f" % (total,pos_num,cost_all,cpm,pos_rate,cpc,cpa))
print("Total   CLICK:%10d, ACTION:%7d COST:%10f CPM:%10f CVR:%10f CPC:%10f" % (total,pos_num,cost_all,cpm,pos_rate,cpc))

#sorted(sub_dict.items(),cmp=lambda x,y : cmp(int(x[1]), int(y[1])),reverse=True)
for k,v in sorted(total_dict.items(),cmp=lambda x,y : cmp(x[0], y[0]),reverse=False):
#    if k.find("cvr"):
#      continue
    click = float(v["clk"])
    action = float(v["action"])
    price = float(v["price"])
    total_cost = price / 1000000.0
    cpm = total_cost / (click + 0.0) * 1000
    cvr = action / (click + 0.0)
    cpc = price/ (action + 0.000001) / 1000000
    copc = action / v["pcvr"]

    print("%10s Model CLICK:%7d ACTION:%7d COST:%10f CPM:%10f CVR:%10f COPC:%10f" % (k,click, action, total_cost, cpm, cvr , copc))

