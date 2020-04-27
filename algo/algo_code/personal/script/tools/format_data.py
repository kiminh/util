#!/bin/env python

import sys
import re
import string
from optparse import OptionParser
if len(sys.argv) < 2:
    print 'must 2 param, base_ute exp_ute'
base_ute = "0"
exp_ute = {}
def format_fun():
    res = {}
    total = {}
    for line in sys.stdin:
        items = line[:-1].split('\t')
        if items[0] != base_ute and items[0] not in exp_ute:
            continue
        key = items[1] + '_' + items[0] 
        if key not in res:
            res[key] = [items[0], int(items[3]), int(items[4]), int(items[5]), int(items[6]), 0]
        else:
            res[key][1] += int(items[3])
            res[key][2] += int(items[4])
            res[key][3] += int(items[5])
            res[key][4] += int(items[6])
        if items[2] == 'PS':
            res[key][5] += int(items[8])

        key = items[0]
        if key not in total:
            total[key] = [items[0], int(items[3]), int(items[4]), int(items[5]), int(items[6]), 0]
        else:
            total[key][1] += int(items[3])
            total[key][2] += int(items[4])
            total[key][3] += int(items[5])
            total[key][4] += int(items[6])
        if items[2] == 'PS':
            total[key][5] += int(items[8])
    for tag in ['LU','BID']:
        key = tag + '_' + base_ute
        if key not in res:
            print 'INVALID:base_ute ute %s not in res' %(base_ute)
            continue
        [bpv1, bpv2, bclk, bgain, bdiv_pv] = [res[key][1], res[key][2], res[key][3], res[key][4], res[key][5]]
        bs1ctr = bpv2*1000.0/bpv1
        bs2ctr = bclk*1000.0/bpv2
        btctr = bclk*1000.0/bpv1
        bs2cpm = bgain*1000/bpv2
        btcpm = bgain*1000/bpv1
        bacp = bgain/bclk
        print '%s\t%s\t%d\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%d\t%d\t%d' %(tag, base_ute, bpv1, bpv2, bclk, bgain, bdiv_pv, bs1ctr, bs2ctr, btctr, bs2cpm, btcpm, bacp)
        
        for ute in exp_ute:
            key = tag + '_' + ute
            if key not in res:
                continue
            [pv1, pv2, clk, gain, div_pv] = [res[key][1], res[key][2], res[key][3], res[key][4], res[key][5]]
            [s1ctr, s2ctr, tctr, s2cpm, tcpm, acp ] = [0]*6
            if pv1 > 0:
                s1ctr = pv2*1000.0/pv1
                tctr = clk*1000.0/pv1
                tcpm = gain*1000/pv1
            if bpv2 > 0:
                s2ctr = clk*1000.0/pv2
                s2cpm = gain*1000/pv2
            if clk > 0:
                acp = gain/clk
            print '%s\t%s\t%d\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%d\t%d\t%d' %(tag, ute, pv1, pv2, clk, gain, div_pv, s1ctr, s2ctr, tctr, s2cpm, tcpm, acp)
            [ratio_pv1, ratio_pv2, ratio_clk, ratio_gain, ratio_div_pv, ratio_s1ctr, ratio_s2ctr, ratio_s2cpm, ratio_tcpm, ratio_acp] = [0]*10
            if bpv1 > 0:
                ratio_pv1 = (pv1 - bpv1)*100.0/bpv1
            if bpv2 > 0:
                ratio_pv2 = (pv2 - bpv2)*100.0/bpv2
            if bclk > 0:
                ratio_clk = (clk - bclk)*100.0/bclk
            if bgain > 0:
                ratio_gain = (gain - bgain) * 100.0/bgain
            if bdiv_pv > 0:
                ratio_div_pv = (div_pv*1.0/pv1 - bdiv_pv*1.0/bpv1)*100.0/(bdiv_pv*1.0/bpv1)
            if bs2ctr > 0:
                ratio_s2ctr = (s2ctr - bs2ctr) * 100.0/bs2ctr
            if bs1ctr > 0:
                ratio_s1ctr = (s1ctr - bs1ctr) * 100.0/bs1ctr
            if btctr > 0:
                ratio_tctr = (tctr - btctr) * 100.0 / btctr
            if bs2cpm > 0:
                ratio_s2cpm = (s2cpm - bs2cpm) * 100.0/bs2cpm
            if btcpm > 0:
                ratio_tcpm = (tcpm - btcpm) * 100.0/btcpm
            if bacp > 0:
                ratio_acp = (acp - bacp) * 100.0/bacp
            print '%s\tchange\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%' %(tag, ratio_pv1, ratio_pv2, ratio_clk, ratio_gain, ratio_div_pv, ratio_s1ctr, ratio_s2ctr, ratio_tctr, ratio_s2cpm, ratio_tcpm, ratio_acp)

    # total
    if base_ute not in total:
        print 'INVALID: base_ute[%s] not in total' %(base_ute)
        return
    [bpv1, bpv2, bclk, bgain, bdiv_pv] = [total[base_ute][1], total[base_ute][2], total[base_ute][3], total[base_ute][4], total[base_ute][5]]
    bs1ctr = bpv2*1000.0/bpv1
    bs2ctr = bclk*1000.0/bpv2
    btctr = bclk*1000.0/bpv1
    bs2cpm = bgain*1000/bpv2
    btcpm = bgain*1000/bpv1
    bacp = bgain/bclk
    print 'total\t%s\t%d\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%d\t%d\t%d' %(base_ute, bpv1, bpv2, bclk, bgain,
            bdiv_pv, bs1ctr, bs2ctr, btctr, bs2cpm, btcpm, bacp)
    for ute in exp_ute:
        if ute not in total:
            continue
        [pv1, pv2, clk, gain, div_pv] = [total[ute][1], total[ute][2], total[ute][3], total[ute][4], total[ute][5]]
        [s1ctr, s2ctr, tctr, s2cpm, tcpm, acp ] = [0]*6
        if pv1 > 0:
            s1ctr = pv2*1000.0/pv1
            tctr = clk*1000.0/pv1
            tcpm = gain*1000/pv1
        if pv2 > 0:
            s2ctr = clk*1000.0/pv2
            s2cpm = gain*1000/pv2
        if clk > 0:
            acp = gain/clk
        print 'total\t%s\t%d\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%d\t%d\t%d' %(ute, pv1, pv2, clk, gain, div_pv, s1ctr, s2ctr, tctr, s2cpm, tcpm, acp)
        [ratio_pv1, ratio_pv2, ratio_clk, ratio_gain, ratio_div_pv, ratio_s1ctr, ratio_s2ctr, ratio_s2cpm, ratio_tcpm, ratio_acp] = [0]*10
        if bpv1 > 0:
            ratio_pv1 = (pv1 - bpv1)*100.0/bpv1
        if bpv2 > 0:
            ratio_pv2 = (pv2 - bpv2)*100.0/bpv2
        if bclk > 0:
            ratio_clk = (clk - bclk)*100.0/bclk
        if bgain > 0:
            ratio_gain = (gain - bgain) * 100.0/bgain
        if bdiv_pv > 0:
            ratio_div_pv = (div_pv*1.0/pv1 - bdiv_pv*1.0/bpv1)*100.0/(bdiv_pv*1.0/bpv1)
        if bs2ctr > 0:
            ratio_s2ctr = (s2ctr - bs2ctr) * 100.0/bs2ctr
        if bs1ctr > 0:
            ratio_s1ctr = (s1ctr - bs1ctr) * 100.0/bs1ctr
        if btctr > 0:
            ratio_tctr = (tctr - btctr) * 100.0 / btctr
        if bs2cpm > 0:
            ratio_s2cpm = (s2cpm - bs2cpm) * 100.0/bs2cpm
        if btcpm > 0:
            ratio_tcpm = (tcpm - btcpm) * 100.0/btcpm
        if bacp > 0:
            ratio_acp = (acp - bacp) * 100.0/bacp
        print 'total\tchange\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%\t%.2f%%' %(ratio_pv1, ratio_pv2, ratio_clk, ratio_gain, ratio_div_pv, ratio_s1ctr, ratio_s2ctr, ratio_tctr, ratio_s2cpm, ratio_tcpm, ratio_acp)
    


if __name__ == "__main__" :
    if len(sys.argv) < 3:
        exit(0)
    base_ute = sys.argv[1]
    utes = sys.argv[2].strip('\n\r').split(',')
    for ute in utes:
        exp_ute[ute] = 1
    format_fun()
                                                     
