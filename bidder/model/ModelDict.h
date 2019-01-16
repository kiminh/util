#ifndef BAYES_RTBKIT_MODELDICT_H
#define BAYES_RTBKIT_MODELDICT_H

#include "soa/service/dual_reloader.h"
#include "bidmax/common/str_util.h"
#include "fea/sparse_fea.h"
#include <map>
#include <iostream>
#include <unordered_map>
#include "gbdt.h"

namespace model_dict
{

using namespace alg;

struct FtrlModel
{
    int init();
    ~FtrlModel()
    { fea_weight_vec.clear(); }
    int load(const char *file_dir, const char *file_name);
    double score(fea::instance &_instance, double bias = 0.0);
    double get_inner(fea::instance &_instance);
    int load_dat(const char *file);
    int load_bin(const char *file);
    std::vector<double> fea_weight_vec;
};

typedef DICT::reloader_t <FtrlModel> FtrlModel_mgr_t;

struct FtrlKVModel
{
    int init();
    ~FtrlKVModel(){ fea_weight_map.clear(); }
    int load(const char *file_dir, const char *file_name);
    float score(fea::instance &_instance, float bias = 0.0);
    float get_inner(fea::instance &_instance);
    int load_dat(const char *file);
    int load_bin(const char *file);
    std::unordered_map<uint32_t, float> fea_weight_map;
};

typedef DICT::reloader_t <FtrlKVModel> FtrlKVModel_mgr_t;

struct FMFtrlModel
{
    int init();
    ~FMFtrlModel()
    {
        fea_weight_vec.clear();
        fm_weight_vec.clear();
    }
    int load(const char *file_dir, const char *file_name);
    double score(fea::instance &_instance, double bias = 0.0);
    //double get_inner(fea::instance &_instance);
    //std::map<uint32_t, double> fea_weight_map;
    std::vector<double> fea_weight_vec;
    std::vector <std::vector<double>> fm_weight_vec;
    uint32_t fm_dim;
    uint32_t fea_num;
};
typedef DICT::reloader_t <FMFtrlModel> FMFtrlModel_mgr_t;

struct GBDTModel
{
    int init();
    int load(const char *file_dir, const char *file_name);
    gbdt m_gbdt;
    int join_fea(fea::instance &, std::vector <uint32_t> &gbdt_out, uint32_t);
    double score(fea::instance &);
};

typedef DICT::reloader_t <GBDTModel> GBDTModel_mgr_t;

}
#endif //BAYES_RTBKIT_MODELDICT_H
