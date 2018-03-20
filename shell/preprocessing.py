import sys

if len(sys.argv) < 3:
  print "Usage:python preprocessing.py train_log train_ins"
  exit(1)

hash_num = 10000007
fp_w = open(sys.argv[2], "w")

#0:instance_id 1:item_id 2:item_category_list 3:item_property_list 4:item_brand_id 5:item_city_id 6:item_price_level 7:item_sales_level 8:item_collected_level 9:item_pv_level 10:user_id 11:user_gender_id 12:user_age_level 13:user_occupation_id 14:user_star_level 15:context_id 16:context_timestamp 17:context_page_id 18:predict_category_property 19:shop_id 20:shop_review_num_level 21:shop_review_positive_rate 22:shop_star_level 23:shop_score_service 24:shop_score_delivery 25:shop_score_description 26:is_trade

item_name = [ "instance_id", "item_id", "item_category_list", "item_property_list", "item_brand_id", "item_city_id", "item_price_level", "item_sales_level", "item_collected_level", "item_pv_level", "user_id", "user_gender_id", "user_age_level", "user_occupation_id", "user_star_level", "context_id", "context_timestamp", "context_page_id", "predict_category_property", "shop_id", "shop_review_num_level" "shop_review_positive_rate", "shop_star_level", "shop_score_service", "shop_score_delivery", "shop_score_description", "is_trade" ]

for raw_line in open(sys.argv[1]):
  line = raw_line.strip("\n\r").split(" ")
  if len(line[18].split(";")) < 2:
    continue
  if len(line) != 27:
    #is test log
    fp_w.write("-1 ")
  else:
    #write is_trade
    fp_w.write(line[-1] + " ")
 
  length = len(line)
  for i in range(1, length-2):
    #is item_category_list
    if i == 2:
      item_category_list = line[i].split(";")
      for item in item_category_list:
        h = hash(item_name[i] + item)
        if h < 0:
          h = -h
        fp_w.write(str(h % hash_num) + " ")

    elif i == 3:
      item_property_list = line[i].split(";")
      for item in item_property_list:
        h = hash(item_name[i] + item)
        if h < 0:
          h = -h
        fp_w.write(str(h % hash_num) + " ")

    elif i == 18:
      predict_category_property = line[i].split(";")
      for cate_prop in predict_category_property:
        
        cate = cate_prop.split(":")[0]
        try:
          prop = cate_prop.split(":")[1].split(",")
        except:
          print line
        for p in prop:
          key = cate + "_" + p
          h = hash(item_name[i] + key)
          if h < 0:
            h = -h
          fp_w.write(str(h % hash_num) + " ")
    else:
      h = hash(item_name[i] + line[i])
      if h < 0:
        h = -h
      fp_w.write(str(h % hash_num) + " ")
  fp_w.write("\n")

fp_w.close()     
