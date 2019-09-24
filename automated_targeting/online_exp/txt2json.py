import json
import sys

with open(sys.argv[1]) as f_in, open(sys.argv[2], 'w') as f_out:
    for raw_line in f_in:
        line = raw_line.strip("\n\r ")
        line_json = {"gaid": line}
        f_out.write(json.dumps(line_json))
        f_out.write("\n")
