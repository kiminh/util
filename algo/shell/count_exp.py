#!/usr/bin/env python 
import sys
import math
import operator

if len(sys.argv) < 2:
	print help
'''pattern=label_index,price,BID,time,auction_id,auction_id,time,exchange,provider,userAgentIP,Medium_id,Medium_Name,Medium_Domain,Medium_Cat,didsha1,didmd5,dpidsha1,dpidmd5,device_carrier,device_make,device_model,device_os,device_osv,device_type,connectiontype,language,location,url,ipAddress,userAgent,hour,weekday,device_string,bid_price,account,bid_agent,meta,creativeID,creativeName
'''
coutn_file = open(sys.argv[1], 'r')

total = 0
pos_num = 0
neg_num = 0
total_price = 0

exp_price={}
exp_ctr_price={}

exp_total={}
exp_click={}
creative_click={}
creative_show={}
creative_cost={}

exp_ctr_total={}
exp_ctr_click={}
traffic_total={}
traffic_click={}

for line in coutn_file:
	fleds = line.strip().split("\001")

	if len(fleds) != 47:
		continue
	price = fleds[1]
	if price == "999":
		continue
	price_num = int(price.split('USD')[0])
	total_price = total_price + price_num
	if creative_cost.has_key(fleds[38]):
		creative_cost[fleds[38]] = creative_cost[fleds[38]] + price_num
	else:
		creative_cost[fleds[38]] = price_num

	'''{"cpc_id":1,"cpm_id":2,"ctr_id":1}'''	
	exp_str = fleds[36]
	if len(exp_str.split(",")) != 3:
		continue
	if len(exp_str.split(",")[0].split(":")) != 2:
		continue
	exp_cpc_id = str(exp_str.split(",")[0].split(":")[1])
	if exp_total.has_key(exp_cpc_id):
		exp_total[exp_cpc_id] = exp_total[exp_cpc_id] + 1
	else:
		exp_total[exp_cpc_id] = 1
	if exp_price.has_key(exp_cpc_id):
		exp_price[exp_cpc_id] = exp_price[exp_cpc_id] + price_num
	else:
		exp_price[exp_cpc_id] = price_num
	
	if creative_show.has_key(fleds[38]):
		creative_show[fleds[38]] = creative_show[fleds[38]] + 1
	else:
		creative_show[fleds[38]] = 1
	
	if traffic_total.has_key(fleds[11]):
		traffic_total[fleds[11]] = traffic_total[fleds[11]] + 1
	else:
		traffic_total[fleds[11]] = 1
	
	exp_ctr_id = str(exp_str.split(",")[2].split(":")[1].rstrip('}'))
	if exp_ctr_total.has_key(exp_ctr_id):
		exp_ctr_total[exp_ctr_id] = exp_ctr_total[exp_ctr_id] + 1
	else:
		exp_ctr_total[exp_ctr_id] = 1

	if exp_ctr_price.has_key(exp_ctr_id):
		exp_ctr_price[exp_ctr_id] = exp_ctr_price[exp_ctr_id] + price_num
	else:
		exp_ctr_price[exp_ctr_id] = price_num

	label = int(fleds[0])
	if label != 1:
		label = 0
		neg_num = neg_num + 1
	else:
		pos_num = pos_num + 1
		if exp_click.has_key(exp_cpc_id):
			exp_click[exp_cpc_id] = exp_click[exp_cpc_id] + 1
		else:
			exp_click[exp_cpc_id] = 1
		if exp_ctr_click.has_key(exp_ctr_id):
			exp_ctr_click[exp_ctr_id] = exp_ctr_click[exp_ctr_id] + 1
		else:
			exp_ctr_click[exp_ctr_id] = 1

		if creative_click.has_key(fleds[38]):
			creative_click[fleds[38]] = creative_click[fleds[38]] + 1
		else:
			creative_click[fleds[38]] = 1
		if traffic_click.has_key(fleds[11]):
			traffic_click[fleds[11]] = traffic_click[fleds[11]] + 1
		else:
			traffic_click[fleds[11]] = 1
coutn_file.close()

for k,v in traffic_total.items():
	ctr = traffic_click.get(k,0) / (v + 0.0)
	click=traffic_click.get(k,0)
	
	print("Traffic %s SHOW:%10d, CLICK:%7d CTR:%10f" % (k,v,click,ctr))

for k,v in creative_show.items():
	ctr = creative_click.get(k,0) / (v + 0.0)
	click=creative_click.get(k,0)
	cost=creative_cost.get(k,0)
	cpc=cost/1000000.0 * 6.15 / (click+0.1)
	cpm=cost/ 1000.0 / v * 6.15
	cost=cost/1000000.0 * 6.15
	print("Creative %s SHOW:%10d, CLICK:%7d COST:%10f CPM:%10f CTR:%10f CPC:%10f" % (k,v,click,cost,cpm,ctr,cpc))

total = neg_num + pos_num
pos_rate = (0.0 + pos_num) / total
cpm = total_price / (total + 0.0) / 1000 * 6.15
cost_all = total_price / 1000000.0 * 6.15
cpc = total_price / 1000000.0 * 6.15 / (pos_num+0.1) 
print("Total   SHOW:%10d, CLICK:%7d COST:%10f CPM:%10f CTR:%10f CPC:%10f" % (total,pos_num,cost_all,cpm,pos_rate,cpc))

for k,v in exp_total.items():
	click = exp_click.get(k, 0)
	show = exp_total.get(k, 0)
	total_cost = exp_price[k] / 1000000.0 * 6.15
	cpm = total_cost / (show + 0.0) * 1000
	total_cost = exp_price[k] / 1000000.0 * 6.15
	ctr = click / (show + 0.0)
	cpc = exp_price[k] / (click + 0.01) / 1000000 * 6.15
	print("CPC Model %s SHOW:%10d, CLICK:%7d COST:%10f CPM:%10f CTR:%10f CPC:%10f" % (k,show, click, total_cost,cpm, ctr, cpc))

for k,v in exp_ctr_total.items():
	cpm = exp_ctr_price[k] / (exp_ctr_total[k] + 0.0) / 1000 * 6.15
	total_cost = exp_ctr_price[k] / 1000000.0 * 6.15
	click = exp_ctr_click.get(k, 0)
	show = exp_ctr_total[k]
	ctr = click / (show + 0.0)
	cpc = exp_ctr_price[k] / (click + 0.01) / 1000000 * 6.15
	show = exp_ctr_total[k]
	print("CTR Model %s SHOW:%10d, CLICK:%7d COST:%10f CPM:%10f CTR:%10f CPC:%10f" % (k,show, click, total_cost,cpm, ctr, cpc))
