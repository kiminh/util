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

void split(std::string line,
           std::vector<float> &fea_vec)
{
  std::stringstream ss;
  ss << line;
  while(ss >> line)
  {
    fea_vec.push_back(static_cast<float>(atof(line.c_str())));
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
    fprintf(stderr,"Usage:%s lr_mode_dat fea_table_dat out_model_dat\n",argv[0]);
    exit(1);
  }

  std::ifstream in_model(argv[1]);  
  std::ifstream fea_is(argv[2]);
  std::ofstream out_model(argv[3]);

  std::string line;
  std::vector<uint64_t> fea_vec;
  std::unordered_map<uint64_t, uint64_t> fea_map;
  int line_cnt = 0;

  //read the fea table
  while(getline(fea_is, line))
  {
    fea_vec.clear();
    split(line,fea_vec);
    if(fea_vec.size() != 2)
        continue;
//    std::cout << fea_vec[0] << " " << fea_vec[1] << std::endl;
    fea_map[fea_vec[0]] = fea_vec[1];
    line_cnt ++;
    if(line_cnt % 100000 == 0) {
      std::cout << "line_cnt : " << line_cnt << std::endl;
    }
  }
  
  std::vector<float> fea_val;
  for(int i = 0;i < 4;i++)
    out_model << 0 << std::endl;
  
  int v_fid = 0;
  while(getline(in_model, line))
  {
    fea_val.clear();
    split(line, fea_val);
    if(fea_val.size() != 3)
        continue;
    if(fea_val[2] != 0.0)
        out_model << fea_map[v_fid] << " " << fea_val[2] << std::endl;
    v_fid ++;
  }

  in_model.close();
  out_model.close();
  fea_is.close();
}
