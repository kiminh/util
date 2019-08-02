/*************************************************************************
    > File Name: combined_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 三 12/31 16:32:25 2014
 ************************************************************************/
#include "anycombined_fea.h"

namespace fea {
    using namespace std;
    using namespace util;

    bool anycombined_fea::init() {
        vector <string> vec;
        util::str_util::split(m_fea_arg.fea_name, "_", vec);

        
        if (vec.size() < 2) {
            return false;
        }
        
        for(size_t i = 0 ; i < vec.size(); i++){
            str_util::trim(vec[i]);
            fea_name_vec.push_back(vec[i]);
        }
        return true;
    }

    bool anycombined_fea::extract_fea(const record &record, fea_result &result) {
        //const vector <uint32_t> &fvalue1 = result.get_fea_value(fea_name1);
        //const vector <uint32_t> &fvalue2 = result.get_fea_value(fea_name2);

        vector<vector<uint32_t> > fvalue_vec;
        for(size_t i = 0; i < fea_name_vec.size(); i++){
            vector <uint32_t> fvalue = result.get_fea_value(fea_name_vec[i]);
            fvalue_vec.push_back(fvalue);
        }

        commit_anycombine_fea(fvalue_vec, result);
        return true;

    }
}

