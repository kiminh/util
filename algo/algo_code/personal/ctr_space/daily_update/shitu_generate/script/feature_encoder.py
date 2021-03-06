#!/usr/bin/env python
from ctypes import *
import ctypes, json

raw = '{"ed_log": {"promoted_app": "", "adtarget": 1, "is_native": false, "pctr_cal": 0.1580033926, "city_id": "73101", "planid": "P857crcw0tih8kkk", "dpid": "", "osv": 8000, "adecpm": 23.70050889, "reqid": "be6ea697-7c3f-49bd-a25c-4b7967a4e1b3", "purchase_bid": 0.0, "orgid": "9", "bid_floor": 0.0, "operator": "", "instl": 0, "ocpc_bid": 0.0, "dpidsha1": "a5ab171d2c5551c4b0552b6280b42b578e08e490", "le": "", "pw": 1280, "ad_style": 1, "make": "GOOGLE", "traffic_source": 2, "slotid": 0, "adxsrc": "naga", "con_t": 0, "adx_ecpm": 23.70050889, "plid": "e8b1454f333aeca06c4d1f24a35bec3b", "ph": 720, "nt": 2, "macsha1": "d76f4f17da290fb7b96d953a1b9a01a733cf4d9d", "adh": 720, "rebate_rate": 0.0, "ip": "95.90.255.47", "adpcvr": 0.0, "adpctr": 0.1580033926, "gsp_price": 0.0, "adw": 1280, "macmd5": "f244cddbc7f5161e9dcd758d0c3d7796", "bid": 0.15, "reqprt": 1567135757777, "phone": "", "mac": "", "didmd5": "af2d48f2495881aed1737bb21017f9b6", "adid": "A8763uphr1n2z58p", "reqtype": "1", "bundle_id": "com.bbm", "dt": 1, "impid": "1", "model": "Nexus One", "is_ocpc": false, "lan_t": 3, "cmod": 1, "campaignid": "C5088nmvund6k7ul", "did": "", "triggerd_expids": "6303_6401_6410_6502_6510_7003", "didsha1": "31f455635a12252f8cedb39cd7533fc58a064bff", "userid": "uid", "applist_len": 0, "pcvr_cal": 0.0, "org_type": 1, "publisher_id": "1001028764", "ifa": "bf737ec7-42a8-4325-8ce5-26ef1fa8495e", "appid": "101000415", "dpidmd5": "285a23d862ebbec44bd5520643e2a9eb", "os": 1}}'

encode = cdll.LoadLibrary('./fe.so').PrepareFeatureOffline
encode.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
encode.restype = ctypes.c_char_p

rst = encode(raw, "le,planid,appid,adid", "feature_config.json", "DUMMY")

print rst