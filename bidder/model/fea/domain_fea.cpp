/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "domain_fea.h"
#include <iostream>

namespace fea
{
using namespace std;
using namespace util;
bool domain_fea::init()
{
    if (m_vec_param.size() != 1) {
        return false;
    } else {
        m_record_index = m_vec_param[0];
        if (m_fea_arg.arg == "2") {
            fea_index = 2;
        } else if (m_fea_arg.arg == "3") {
            fea_index = 3;
        } else {
            return false;
        }
    }
    return true;
}

bool domain_fea::extract_fea(const record &record, fea_result &result)
{
    vector <string> vec;
    util::str_util::split(record.valueAt(m_record_index), ".", vec);
    if (vec.size() < 1) {
        return false;
    }
    string fea = vec[0];
    for (uint i = 1; i < vec.size() && i < fea_index; i++) {
        fea += "." + vec[i];
    }

    commit_single_fea(fea, result);
    return true;
}
}
