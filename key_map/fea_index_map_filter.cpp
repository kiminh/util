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

struct node{
  int fid; // virtual feature index
  int cnt; // count of the feature index

  node(){fid = -1; cnt = 0;}
  ~node(){}
};

int main(int argc,char **argv)
{
  if(argc < 4){
    fprintf(stderr,"Usage:%s in_train_ins out_train_ins fea_table\n",argv[0]);
    exit(1);
  }

  int cnt_th = 1;
  std::ifstream is(argv[1]);  
  std::ofstream out_ins(argv[2]);
  std::ofstream out_fea(argv[3]);

  std::string line;
  std::vector<uint64_t> fea_vec;
  std::vector<int> slot_vec;
  std::unordered_map<uint64_t,node> fea_map;
  uint64_t count = 0;
  int line_cnt = 0;

  while(getline(is,line))
  {
    fea_vec.clear();
    split(line,fea_vec);
    out_ins << fea_vec[0] << " ";
    for(size_t i = 1;i < fea_vec.size();++i){
      //if the key is not in the map, add to map 
      //but don't assign the virtual feature id
      if(!fea_map.count(fea_vec[i])){
        node n_;
        n_.cnt = 1;
        fea_map[fea_vec[i]] = n_;
      }else{
        node& n_ = fea_map[fea_vec[i]];
        //if reach the threshold, assign the virtual feature id
        if(n_.cnt == (cnt_th + 1))
        {
          n_.fid = count;
          out_ins << count << " ";
          count ++;
          n_.cnt ++;
        }else if(n_.cnt > (cnt_th + 1)){
          out_ins << n_.fid << " ";
        }else{
          n_.cnt ++;
        }
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
    if(it.second.fid != -1)
      out_fea << it.second.fid << " " << it.first << std::endl;
  }
  out_fea.close();
}
