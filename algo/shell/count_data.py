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

prov_show={}
prov_click={}
prov_cost={}

for line in coutn_file:
	fleds = line.strip().split("\001")
	price = fleds[1]
	if price == "999":
		continue
	price_num = int(price.split('USD')[0])
	'''1:100010:1000034::-1:-1'''
	location_str = fleds[26]
	if len(location_str.split(":")) != 6:
		continue
	prov=location_str.split(":")[1]
	
	prov=fleds[10]
	if prov_cost.has_key(prov):
		prov_cost[prov] = prov_cost[prov] + price_num
	else:
		prov_cost[prov] = price_num
	if prov_show.has_key(prov):
		prov_show[prov] = prov_show[prov] + 1
	else:
		prov_show[prov] = 1

	label = int(fleds[0])
	if label != 1:
		label = 0
		neg_num = neg_num + 1
	else:
		pos_num = pos_num + 1
		if prov_click.has_key(prov):
			prov_click[prov] = prov_click[prov] + 1
		else:
			prov_click[prov] = 1

coutn_file.close()

print("Name,SHOW,CLICK,COST,CPM,CTR")
for k,v in prov_show.items():
	cpm = prov_cost[k] / (prov_show[k] + 0.0) / 1000 * 6.15
	total_cost = prov_cost[k] / 1000000.0 * 6.15
	prov_k = prov_click.get(k, 0.01)
	ctr = prov_k / (prov_show[k] + 0.0)
	show = prov_show[k]
	click = prov_k
	if int(show) > 3000:
		print("%s,%d,%d,%f" % (k,show, click, ctr))
