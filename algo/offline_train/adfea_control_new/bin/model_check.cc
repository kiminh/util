#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <unordered_map>
#include <cmath>
#include <fstream>


void Split(std::string line, std::vector<std::string> &f_vec)
{
  std::stringstream ss;
  ss << line;
  while(ss >> line)
  {
    f_vec.push_back(line);
  }
}

float Eval(const std::unordered_map<uint32_t, float> &model_dict,
           const std::vector<std::string> &f_vec)
{
  float sum = 0.;
  int label = static_cast<int>(atoi(f_vec[0].c_str()));
  for(size_t i = 1;i < f_vec.size();++i)
  {
    uint32_t key = static_cast<uint32_t>(atoi(f_vec[i].c_str()));
    if(model_dict.count(key))
      sum += model_dict.at(key);
  }
  float prob = 1. / (1 + std::exp(-sum));
  if(label == 1)
    return std::log(prob);
  else
    return std::log(1 - prob);
}

int main(int argc, char **argv)
{

  if(argc < 4)
  {
    fprintf(stderr, "Usage:%s model_file increa_shitu result\n",argv[0]);
    exit(1);
  }

  int ret = 0;
  //beta0 feature index
  uint32_t beta0_index = 3007242330;

  long long s_w = 0; // sum of the weight
  long long nnz_w = 0; // sum of the non-zero weight
  float l_f = 0.; // loss function
  float r_w_h = -1.; // max of rang of the weight
  float r_w_l = 1.; // min of rang of the weight

  std::unordered_map<uint32_t, float> model_dict;

  std::ifstream model_file(argv[1]);
  std::ifstream incre_file(argv[2]);
  std::ofstream result(argv[3]);

  float tmp;
  model_file >> tmp >> tmp >> tmp >> tmp;
  //load model from file
  std::string line;
  std::vector<std::string> m_vec;
  model_dict.reserve(45000000);

  while(getline(model_file, line))
  {
    m_vec.clear();
    Split(line, m_vec);
    if(m_vec.size() < 2){
      continue;
    }
    uint32_t key = static_cast<int32_t>(atoi(m_vec[0].c_str()));
    float w = static_cast<float>(atof(m_vec[1].c_str()));

    if(w != 0)
    {
      model_dict[key] = w;
      ++nnz_w;
      if(w > r_w_h)
        r_w_h = w;
      if(w < r_w_l)
        r_w_l = w;
    }
    ++s_w;
    if(s_w % 1000000 == 0)
      std::cout << "load " << s_w << " model key from file." << std::endl;
  }

  model_file.close();
  //cal loss function
  int n_samples = 0;
  while(getline(incre_file, line)){
    m_vec.clear();
    Split(line, m_vec);
    l_f += Eval(model_dict, m_vec);
    ++n_samples;
  }

  incre_file.close();
  //check beta0 feature
  /*float beta0_weight = 0.;
  if (model_dict.count(beta0_index))
    beta0_weight = model_dict[beta0_index];
  else{
    result << "beta0[" << beta0_index <<"] feature has no weight." << std::endl; 
    ret = 1;
  }
  if (beta0_weight == 0.){
    result << "beta0[" << beta0_index <<"] feature has no weight." << std::endl;
    ret = 1;
  }*/
  if(model_dict.count(beta0_index))
    result << "model_beta0=" << model_dict[beta0_index] << std::endl;
  else
    result << "model_beta0=" << -1 << std::endl;
  result << "model_keys=" << s_w << std::endl;
  result << "model_weight_nzero=" << nnz_w << std::endl;
  result << "model_loss=" << - l_f / n_samples << std::endl;
  result << "model_weight_high=" << r_w_h << std::endl;
  result << "model_weight_low=" << r_w_l << std::endl;

  result.close();
  return ret;
}
