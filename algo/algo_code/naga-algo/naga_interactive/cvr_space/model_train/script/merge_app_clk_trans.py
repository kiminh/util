import sys

if len(sys.argv) < 3:
    print "Usage: py input output"
    exit(1)

appclktrans = {}
for raw in open(sys.argv[1]):
    line = raw.strip().split('\t')
    if len(line) < 3:
        continue
    app_name = line[0]
    click = line[1]
    trans = line[2]

    if app_name not in appclktrans:
        appclktrans[app_name] = {"click": 0, "trans": 0}
    appclktrans[app_name]["click"] += int(click)
    appclktrans[app_name]["trans"] += int(trans)

#om.teamorb.android^I1.0#0.1$
with open(sys.argv[2], 'w') as f_out:
    for app, value in appclktrans.items():
        if value["trans"] > 30:
            f_out.write("%s\t1.0#0.1\n" % app)

