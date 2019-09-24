import json
import sys
import random
import numpy as np

user2app = dict()
app2user = dict()
i = 1
#with open(sys.argv[1]) as f_in:
for raw_line in sys.stdin:
    line_json = json.loads(raw_line.strip("\n\r "))
    gaid = line_json['gaid']
    pkg_name = line_json['pkg_name']
    #idf = line_json['idf']
    if gaid not in user2app:
        user2app[gaid] = {}
    if pkg_name not in user2app[gaid]:
        user2app[gaid][pkg_name] = 1#idf

    if pkg_name not in app2user:
        app2user[pkg_name] = set()
    app2user[pkg_name].add(gaid)
    i+=1
    if i % 10000 == 0:
        print i
    if i == 1000000:
        break

max_path = 20
sentences = []
length = []
for gaid in user2app:
    sentence = [gaid]
    while len(sentence) != max_path:
        value_list = []
        for key, value in user2app[gaid].items():
            value_list += int(value)*[key]
        pick_value = random.choice(value_list)
        pick_users = app2user[pick_value] - set(gaid)
        if len(pick_users) == 0:
            break
        pick_gaid = random.choice(list(pick_users))
        if len(sentence) >= 2 and sentence[-2] == pick_gaid:
            break
        else:
            sentence.append(pick_gaid)
        gaid = pick_gaid
    sentences.append(sentence)
    length.append(len(sentence))
    if len(sentences) % 100000 == 0:
        print(len(sentences))

print(np.mean(length))
print(len(sentences))

with open('user_sequence.json', 'w') as f_out:
    for sentence in sentences:
        res = {}
        res['user_list'] = sentence
        f_out.write(json.dumps(res))
        f_out.write("\n")
