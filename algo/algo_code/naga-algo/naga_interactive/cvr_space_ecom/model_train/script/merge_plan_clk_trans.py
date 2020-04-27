import sys

if len(sys.argv) < 3:
    print "Usage: py input output"
    exit(1)

planclktrans = {}
for raw in open(sys.argv[1]):
    line = raw.strip().split('\t')
    if len(line) < 3:
        continue
    plan_id = line[0]
    click = line[1]
    trans = line[2]

    if plan_id not in planclktrans:
        planclktrans[plan_id] = {"click": 0, "trans": 0}
    planclktrans[plan_id]["click"] += int(click)
    planclktrans[plan_id]["trans"] += int(trans)

#planid^I1.0#0.1$
with open(sys.argv[2], 'w') as f_out:
    for plan, value in planclktrans.items():
        if value["trans"] > 30:
            f_out.write("%s\t1.0#0.1\n" % plan)

