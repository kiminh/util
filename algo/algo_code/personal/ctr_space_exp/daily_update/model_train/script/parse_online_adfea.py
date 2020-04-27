import json
import sys

for raw_line in open(sys.argv[1]):
    line_json = json.loads(raw_line.strip('\n\r '))
    
    line_sort = sorted(line_json.items(), key=lambda d: int(d[0].split(':')[0]))
    for key, value in line_sort:
        print key, value
