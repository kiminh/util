#!/usr/bin/python
import sys
import json

if len(sys.argv) < 3:
  print "Usage : fearemap gbdt_model.json fea_table output"
  exit(1)

fea_dict = {}

def convert(input):
  if isinstance(input, dict):
      return {convert(key): convert(value) for key, value in input.iteritems()}
  elif isinstance(input, list):
      return [convert(element) for element in input]
  elif isinstance(input, unicode):
      return input.encode('utf-8')
  else:
      return input

def convert_one_tree(subtree):
  if "leaf" in subtree:
    return

  split = subtree["split"]
  subtree["split"] = fea_dict[split]
  convert_one_tree(subtree["children"][0])
  convert_one_tree(subtree["children"][1])

if __name__ == "__main__":
  for l in open(sys.argv[2],"r"):
    arr = l.strip("\r\n").split(" ")
    fea_dict[int(arr[0])] = int(arr[1])

  fw = open(sys.argv[3],"w")
  fr = open(sys.argv[1],"r")

  buff = fr.read();
  model_dict = convert(json.loads(buff))

  for i in range(len(model_dict)):
    convert_one_tree(model_dict[i])

  file_data = json.dumps(model_dict, ensure_ascii=False, sort_keys=True, indent=4)
  #file_data = json.dumps(model_dict)
  fw.write(file_data)
  fw.close()
