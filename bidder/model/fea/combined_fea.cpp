/*************************************************************************
    > File Name: combined_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 三 12/31 16:32:25 2014
 ************************************************************************/
#include "combined_fea.h"

namespace fea
{
using namespace std;

bool combined_fea::init()
{
    vector <string> vec;
    util::str_util::split(m_fea_arg.fea_name, "_", vec);

    if (vec.size() != 2) {
        return false;
    }
    fea_name1 = vec[0];
    fea_name2 = vec[1];
    util::str_util::trim(fea_name1);
    util::str_util::trim(fea_name2);
    return true;
}

bool combined_fea::extract_fea(const record &record, fea_result &result)
{
    const vector <uint32_t> &fvalue1 = result.get_fea_value(fea_name1);
    const vector <uint32_t> &fvalue2 = result.get_fea_value(fea_name2);

    for (size_t i = 0; i < fvalue1.size(); ++i) {
        for (size_t j = 0; j < fvalue2.size(); ++j) {
            commit_combine_fea(fvalue1[i], fvalue2[j], result);
        }
    }
    return true;
}
}

