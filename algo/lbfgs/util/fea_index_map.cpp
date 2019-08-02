//for no hash feature index map to a sequence
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>

void split(std::string line,
           std::vector<uint64_t> &fea_vec)
{
  std::stringstream ss;
  ss << line;
  while(ss >> line)
  {
    fea_vec.push_back(static_cast<uint64_t>(atol(line.c_str())));
  }
}

int main(int argc,char **argv)
{
  if(argc < 4){
    fprintf(stderr,"Usage:%s in_train_ins out_train_ins fea_table\n",argv[0]);
    exit(1);
  }

  std::ifstream is(argv[1]);  
  std::ofstream out_ins(argv[2]);
  std::ofstream out_fea(argv[3]);

  std::string line;
  std::vector<uint64_t> fea_vec;
  std::vector<int> slot_vec;
  std::unordered_map<uint64_t,uint64_t> fea_map;
  uint64_t count = 0;
  int line_cnt = 0;

  while(getline(is,line))
  {
    fea_vec.clear();
    split(line,fea_vec);
    out_ins << fea_vec[0] << " ";
    for(size_t i = 1; i < fea_vec.size();++i){
      if(!fea_map.count(fea_vec[i])){
        fea_map[fea_vec[i]]=count;
        out_ins << count << " ";
        count++;
      }else{
        out_ins << fea_map[fea_vec[i]] << " ";
      }
    }
    out_ins << std::endl;
    line_cnt ++;
    if(line_cnt % 100000 == 0) {
      std::cout << "line_cnt : " << line_cnt << std::endl;
    }
  } 
  is.close();
  out_ins.close();

  out_fea << fea_map.size() << std::endl;

  for(const auto &it : fea_map){
    out_fea << it.second << " " << it.first << std::endl;
  }
  out_fea.close();
}
