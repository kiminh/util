/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "installed_num_fea.h"
#include <iostream>

namespace fea
{
using namespace std;
using namespace util;

bool installed_num_fea::init()
{
    if (m_vec_param.size() != 1) {
        return false;
    } else {
        m_record_index = m_vec_param[0];
        m_cut = atoi(m_fea_arg.arg.c_str());
    }
    return true;
}

bool installed_num_fea::extract_fea(const record &record, fea_result &result)
{
    vector <string> vec;
    int num = atoi(record.valueAt(m_record_index).c_str());
    if (num > m_cut) {
        num = m_cut;
    }

    char tmp[128];
    snprintf(tmp, 128, "%i", num);
    commit_single_fea(string(tmp), result);
    return true;
}
}
