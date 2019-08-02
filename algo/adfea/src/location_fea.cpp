/*************************************************************************
    > File Name: location_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: Mon 20 Apr 2015 03:58:53 PM CST
 ************************************************************************/

#include "location_fea.h"
#include "str_util.h"
#include "fea_base.h"

using namespace std;
using namespace util;

//1:100018:1000089::-1:-1
bool fea::location_fea::init() {
    if (m_vec_param.size() != 1) {
        return false;
    }
    else {
        location_index = m_vec_param[0];
        if (m_fea_arg.arg == "p") {
            fea_index = 1;
        }
        else if (m_fea_arg.arg == "v") {
            fea_index = 2;
        }
        else if (m_fea_arg.arg == "c") {
            fea_index = 0;
        }
        else {
            return false;
        }
    }
    return true;
}

bool fea::location_fea::extract_fea(const record &record, fea_result &result) {
    vector<string> vec;
    util::str_util::split(record.valueAt(location_index), ":", vec);
    if (vec.size() < 3) {
        is_extract = false;
        return false;
    }

    commit_single_fea(vec[fea_index], result);
    //is_extract = true;
    return true;
}
