import sys

index = 0
MIN_PV = 30 
for line in sys.stdin:
    flds = line.strip().split('\t')
    if len(flds) != 4:
        continue
    (fea_sign, fea_text, pv, click) = flds[0:4]
    if int(pv) < MIN_PV: continue
    print "%s\t%s" % (index, fea_sign)
    index += 1
