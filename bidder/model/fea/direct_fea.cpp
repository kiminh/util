/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "direct_fea.h"
#include <iostream>
namespace fea
{
using namespace std;
using namespace util;
bool direct_fea::init()
{
    if (m_vec_param.size() != 1) {
        return false;
    } else {
        m_record_index = m_vec_param[0];
    }
    return true;
}

bool direct_fea::extract_fea(const record &record, fea_result &result)
{
    string fea(record.valueAt(m_record_index));
    transform(fea.begin(), fea.end(), fea.begin(), ::tolower);
    commit_single_fea(fea, result);
    return true;
}
}
