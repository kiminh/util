import json

pkg_json = json.load(open("pkg_labels.json"))
labels = pkg_json['labels']
with open('pkg_index.txt', 'w') as f_out:
    for pkg in labels:
        f_out.write("%s\n" % pkg)
 
