#ifndef BAYES_RTBKIT_MODELDICT_H
#define BAYES_RTBKIT_MODELDICT_H

#include <map>
#include "str_util.h"
#include <iostream>

namespace model_dict {

    struct FMFtrlModel {
    	 int init();
    	 ~FMFtrlModel(){fea_weight_vec.clear();fm_weight_vec.clear();}
    	 int load(const char *file_name);
    	 double score(std::vector<uint32_t>,double bias=0.0);
    	 //double get_inner(fea::instance &_instance);
    	  //std::map<uint32_t, double> fea_weight_map;
    	std::vector<double> fea_weight_vec;
    	std::vector<std::vector<double>> fm_weight_vec;
    	uint32_t fm_dim;
    	uint32_t fea_num;
    };
    typedef struct FMFtrlModel FMFtrlModel_t;
}

#endif //BAYES_RTBKIT_MODELDICT_H
