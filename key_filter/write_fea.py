import sys

fp_w = open(sys.argv[2],"w")

key_dict = {}

for raw_line in open(sys.argv[1],"r"):
  line = raw_line.strip().split(" ")
  for l in line:
    if int(l) != 0 and int(l) != 1:
      if l not in key_dict:
        key_dict[l] = 1
      else:
        key_dict[l] += 1

for key,value in key_dict.items():
  fp_w.write(key+":"+str(value)+"\n")
