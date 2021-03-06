#ifndef _BAYESALGO_FTRL_TRAIN_
#define _BAYESALGO_FTRL_TRAIN_

#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <cmath>
#include <cstring>
#include <fstream>
#include <random>
#include <cassert>

#include "ffm.h"
#include "str_util.h"
#include "metric.h"
#include "elapse.h"

namespace BayesAlgo {

class FTRL {
public:
  explicit FTRL(char *dtrain)
    :dtrain(dtrain),n(nullptr),
    z(nullptr),n_ffm(nullptr),z_ffm(nullptr)
   {
    alpha = 0.01f;
    beta = 1.0f;
    alpha_ffm = 0.01f;
    beta_ffm = 1.0f;

    l1_reg = 0.0f;
    l2_reg = 0.1f;
    l1_ffm_reg = 0.0f;
    l2_ffm_reg = 0.1f;
    task = "train";
    model_out = "ffm_model.dat";
    model_in = "NULL";
    num_epochs = 1;
  }

  virtual ~FTRL() 
  {
    if(z)
      delete [] z;
    if(n)
      delete [] n;
    if(z_ffm)
      delete [] z_ffm;
    if(n_ffm)
      delete [] n_ffm;
  }

  inline void Init()
  {
    ffm.Init();
    size_t ftrl_param_size = ffm.param.n * ffm.param.m * ffm.param.d;
    ffm_model_size = ffm.GetModelSize();

    ValueType memory_use = (ffm.param.n * 3 + ftrl_param_size * 3) * sizeof(ValueType) * 1.0 / 1024 / 1024 / 1024;
    std::cout << "num_fea=" << ffm.param.n << ", ffm_dim=" << ffm.param.d << ", num_field="<< ffm.param.m 
              << ", num_epochs=" << num_epochs << ", use_memory=" << memory_use << " GB" << std::endl;
    
    std::cout << "alpha=" << alpha << ", beta=" << beta << ", alpha_ffm=" << alpha_ffm << ", beta_ffm=" << beta_ffm
              << ", l1_reg=" << l1_reg << ", l2_reg=" << l2_reg << ", l1_ffm_reg=" << l1_ffm_reg << ", l2_ffm_reg=" << l2_ffm_reg << std::endl;

    z = new ValueType[ffm.param.n + 1];
    n = new ValueType[ffm.param.n + 1];
    z_ffm = new ValueType[ftrl_param_size];
    n_ffm = new ValueType[ftrl_param_size];

    memset(z, 0.0, (ffm.param.n + 1) * sizeof(ValueType));
    memset(n, 0.0, (ffm.param.n + 1) * sizeof(ValueType));
    memset(n_ffm, 0.0, (ftrl_param_size) * sizeof(ValueType));

    p_gauss_distribution = new std::normal_distribution<ValueType>(0.0,0.01);
    for(size_t i = 0;i < ftrl_param_size;++i){
      z_ffm[i] = (*p_gauss_distribution)(generator);
    }
  }

  inline void SetParam(const char *name,const char *val) {
    if(!strcmp(name,"alpha"))
      alpha = static_cast<float>(atof(val));
    if(!strcmp(name,"beta"))
      beta = static_cast<float>(atof(val));
    if(!strcmp(name,"alpha_ffm"))
      alpha_ffm = static_cast<float>(atof(val));
    if(!strcmp(name,"beta_ffm"))
      beta_ffm = static_cast<float>(atof(val));
    if(!strcmp(name,"l1_reg"))
      l1_reg = static_cast<float>(atof(val));
    if(!strcmp(name,"l2_reg"))
      l2_reg = static_cast<float>(atof(val));
    if(!strcmp(name,"l1_ffm_reg"))
      l1_ffm_reg = static_cast<float>(atof(val));
    if(!strcmp(name,"l2_ffm_reg"))
      l2_ffm_reg = static_cast<float>(atof(val));
    if(!strcmp(name,"task"))
      task = val;
    if(!strcmp(name,"model_out"))
      model_out = val;
    if(!strcmp(name,"model_in"))
      model_in = val;
    if(!strcmp(name,"num_epochs"))
      num_epochs = static_cast<unsigned>(atoi(val));

    ffm.SetParam(name,val);
  }

  inline void TaskTrain()
  {
    Timer t;
    t.Start();
    std::string line;
    Instance ins;
    for(unsigned i = 0;i < num_epochs;++i) {

      std::ifstream train_stream(dtrain);
      assert(train_stream.fail() == false);

      int cnt = 0;

      while(getline(train_stream,line)) {
        ins.clear();
        ParseLine(line,ins);
        UpdateOneIter(ins);
        cnt++;
        if(cnt % 100000 == 0)
        {  
          std::cout << "train instance : " << cnt << std::endl;
        }
      }
    }
    t.Stop();
    std::cout << "Elapsed time:" << t.ElapsedSeconds() << " sec." << std::endl;
    DumpModel();
  }

    // 23:123444 v.index[j]:v.get_value(j) 
  inline ValueType PredIns(Instance ins) {
 
    size_t ins_len = ins.fea_vec.size();
    std::vector<ffm_node> &fea_vec = ins.fea_vec;

    ValueType inner = ffm.w[ffm_model_size - 1];
    for(size_t i = 0;i < ins_len;++i)
    {
      uint32_t fea_index = fea_vec[i].fea_index;
      inner += ffm.w[fea_index];
    }

    for(size_t i = 0;i < ins_len;++i)
    {
      uint32_t fea_x = fea_vec[i].fea_index;
      uint32_t field_x = fea_vec[i].field_index;

      for(size_t j = i+1;j < ins_len;++j) {
        uint32_t fea_y = fea_vec[j].fea_index;
        uint32_t field_y = fea_vec[j].field_index;
        uint32_t real_fea_y =
                  ffm.param.n + (fea_y) * ffm.param.m * ffm.param.d + (field_x - 1) * ffm.param.d;
        uint32_t real_fea_x =
                  ffm.param.n + (fea_x) * ffm.param.m * ffm.param.d + (field_y - 1) * ffm.param.d;

        if(i!=j){
          for(size_t k = 0;k < ffm.param.d;++k) {
            inner += ffm.w[real_fea_x + k] * ffm.w[real_fea_y + k];
          }
        }
      }
    }
    return Sigmoid(inner);
  }

  void TaskPred()
  {
      std::ifstream is(dtrain);
      assert(is.fail() == false);

      this->LoadModel();
      pair_vec.clear();
      std::string line;
      Instance ins;
      while(getline(is,line)) {
        ins.clear();
        ParseLine(line,ins);
        float pv = PredIns(ins);
        Metric::pair_t p(pv,ins.label);
        pair_vec.push_back(p);
      }
      std::cout << "Test AUC=" << Metric::CalAUC(pair_vec) 
                << ",COPC=" << Metric::CalCOPC(pair_vec);
  }

  virtual void ParseLine(const std::string &line,Instance &ins)
  {
    std::vector<std::string> fea_vec;
    util::str_util::split(line," ",fea_vec);
    ins.label = static_cast<int>(atoi(fea_vec[0].c_str()));

    for(size_t i = 1;i < fea_vec.size();i++) {
      std::vector<std::string> kvs;
      ffm_node _node;
      util::str_util::split(fea_vec[i],":",kvs);

      _node.field_index = static_cast<uint32_t>(atoi(kvs[0].c_str()));
      _node.fea_index = static_cast<uint32_t>(atoi(kvs[1].c_str()));

      assert(_node.field_index <= ffm.param.m);
      assert(_node.fea_index < ffm.param.n);

      ins.fea_vec.push_back(_node);
    }
  }

  virtual ValueType PredictRaw(Instance &ins)
  {
    size_t ins_len = ins.fea_vec.size();
    std::vector<ffm_node> &fea_vec = ins.fea_vec;
    ValueType sum = 0.0;
    //w_0 update
    ffm.w[ffm_model_size - 1] = ( - z[ffm.param.n]) / \
                ((beta + std::sqrt(n[ffm.param.n])) / alpha);
    sum += ffm.w[ffm_model_size - 1];
    //w_i update
    for(size_t index = 0;index < ins_len;++index)
    {
      uint32_t fea_index = fea_vec[index].fea_index;
      if(std::fabs(z[fea_index]) < l1_reg) {
        ffm.w[fea_index] = 0.0;
      }else{
        ffm.w[fea_index] = (Sign(z[fea_index]) * l1_reg - z[fea_index]) / \
                      ((beta + std::sqrt(n[fea_index])) / alpha + l2_reg);
      }
      sum += ffm.w[fea_index];
    }

    for(size_t i = 0;i < ins_len;++i) {
      uint32_t fea_x = fea_vec[i].fea_index;
      for(size_t j = 0;j < ins_len;++j) {
        uint32_t field_y = fea_vec[j].field_index;
        if(i != j) {
          for(size_t k = 0;k < ffm.param.d;++k) {
            uint32_t real_fea_index =
                    (fea_x) * ffm.param.m * ffm.param.d + (field_y - 1) * ffm.param.d + k;
            uint32_t map_fea_index = real_fea_index + ffm.param.n;

            if(std::fabs(z_ffm[real_fea_index]) < l1_ffm_reg){
              ffm.w[map_fea_index] = 0.0;
            }else{
              ffm.w[map_fea_index] = (Sign(z_ffm[real_fea_index]) * l1_ffm_reg - z_ffm[real_fea_index]) / \
                            ((beta_ffm + std::sqrt(n_ffm[real_fea_index])) / alpha_ffm + l2_ffm_reg);
            }
          }
        }
      }
    }

    for(size_t i = 0;i < ins_len;++i)
    {
      uint32_t fea_x = fea_vec[i].fea_index;
      uint32_t field_x = fea_vec[i].field_index;

      for(size_t j = i+1;j < ins_len;++j) {
        if(i != j) {
          uint32_t fea_y = fea_vec[j].fea_index;
          uint32_t field_y = fea_vec[j].field_index;

          uint32_t real_fea_y =
                    ffm.param.n + (fea_y) * ffm.param.m * ffm.param.d + (field_x - 1) * ffm.param.d;
          uint32_t real_fea_x =
                    ffm.param.n + (fea_x) * ffm.param.m * ffm.param.d + (field_y - 1) * ffm.param.d;

          for(size_t k = 0;k < ffm.param.d;++k) {
            sum += ffm.w[real_fea_x + k] * ffm.w[real_fea_y + k];
          }
        }
      }
    }
    return sum;
  }

  virtual ValueType Predict(ValueType inx) {
    return Sigmoid(inx);
  }

  inline int Sign(ValueType inx)
  {
    return inx > 0?1:0;
  }

  inline ValueType Sigmoid(ValueType inx)
  {
    assert(!std::isnan(inx));
    ValueType tuc_val = 31;
    return 1. / (1. + std::exp(-std::max(std::min(inx,tuc_val),-tuc_val)));
  }

  virtual void AuxUpdate(const Instance &ins,ValueType grad)
  {
    size_t ins_len = ins.fea_vec.size();
    std::vector<ffm_node> fea_vec = ins.fea_vec;

    ValueType sigma = (std::sqrt(n[ffm.param.n] + grad * grad) - std::sqrt(n[ffm.param.n])) / alpha;
    z[ffm.param.n] += grad - sigma * ffm.w[ffm_model_size - 1];
    n[ffm.param.n] += grad * grad;

    for(size_t index = 0;index < ins_len;++index)
    {
      uint32_t fea_index = fea_vec[index].fea_index;
      ValueType theta = (std::sqrt(n[fea_index] + grad * grad) - std::sqrt(n[fea_index])) / alpha;
      z[fea_index] += grad - theta * ffm.w[fea_index];
      n[fea_index] += grad * grad;
    }
    std::map<uint32_t,ValueType> sum_ffm;
    for(size_t i = 0;i < ins_len;++i)
    {
      uint32_t fea_x = fea_vec[i].fea_index;
      uint32_t field_x = fea_vec[i].field_index;

      for(size_t j = 0; j < ins_len;++j)
      {
        if(i != j) {
          uint32_t fea_y = fea_vec[j].fea_index;
          uint32_t field_y = fea_vec[j].field_index;
          uint32_t real_fea_y =
                    ffm.param.n + (fea_y) * ffm.param.m * ffm.param.d + (field_x - 1) * ffm.param.d;
          uint32_t real_fea_x =
                    ffm.param.n + (fea_x) * ffm.param.m * ffm.param.d + (field_y - 1) * ffm.param.d;

          for(size_t k = 0;k < ffm.param.d;++k) {
            uint32_t real_index = real_fea_x + k;
            if(sum_ffm.find(real_index) != sum_ffm.end())
              sum_ffm[real_index] += ffm.w[real_fea_y + k];
            else
              sum_ffm[real_index] = ffm.w[real_fea_y + k];
          }
        }
      }
    }

    for(size_t i = 0;i < ins_len;++i) {
      uint32_t fea_x = fea_vec[i].fea_index;
      for(size_t j = 0;j < ins_len;++j) {
        uint32_t field_y = fea_vec[j].field_index;
        if(i != j){
          for(size_t k = 0;k < ffm.param.d;++k){
            uint32_t real_fea_index =
                      (fea_x) * ffm.param.m * ffm.param.d + (field_y - 1) * ffm.param.d + k;
            uint32_t map_fea_index = real_fea_index + ffm.param.n;
            ValueType g_ffm = grad * sum_ffm[map_fea_index];
            ValueType theta = (std::sqrt(n_ffm[real_fea_index] + g_ffm * g_ffm) - std::sqrt(n_ffm[real_fea_index])) / alpha_ffm;
            z_ffm[real_fea_index] += g_ffm - theta * ffm.w[map_fea_index];
            n_ffm[real_fea_index] += g_ffm * g_ffm;
          }
        }
      }
    }
  }

  virtual void UpdateOneIter(Instance &ins)
  {
    ValueType p = Predict(PredictRaw(ins));
    int label = ins.label;
    ValueType grad = p - label;
    AuxUpdate(ins,grad);
  }

  virtual void Run()
  {
    if(task == "train") {    
      this->Init();
      this->TaskTrain();
    }else if(task == "pred") {
      std::cout << "Load FFM Model now...";
      //this->LoadModel(model_in.c_str());
      //this->TaskPred();
    }else
      std::cerr << "error task!";
  }

  virtual void DumpModel() {
    std::ofstream os(model_out.c_str());
    assert(os.fail() == false);
    ffm.DumpModel(os);
    os.close();
  }

  virtual void LoadModel() {
    std::ifstream is(model_in.c_str());
    assert(is.fail() == false);
    ffm.LoadModel(is);
    is.close();
  }

private:
  char *dtrain;

  FFMModel ffm;
  ValueType *n,*z;
  ValueType *n_ffm,*z_ffm;

  float alpha,beta;
  float alpha_ffm,beta_ffm;

  float l1_reg,l2_reg;
  float l1_ffm_reg,l2_ffm_reg;

  std::string task;
  std::string model_out;
  std::string model_in;

  unsigned num_epochs;
  size_t ffm_model_size;

  std::vector<Metric::pair_t> pair_vec;
  std::default_random_engine generator;
  std::normal_distribution<ValueType> *p_gauss_distribution;
}; //end class

} // end namespace
#endif
