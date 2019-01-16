/*************************************************************************
    > File Name: lr_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一  1/ 5 15:11:09 2015
 ************************************************************************/

#include <map>
#include <iostream>
#include "fea_base.h"

namespace fea
{
using namespace std;

void fea_base::commit_single_fea(const string &value, fea_result &result)
{
    uint32_t sign = hash_fn(m_fea_arg.fea_name + value);
//        cout << "sign " << sign << " fea_name" << m_fea_arg.fea_name + value << endl;
    fea_t &fea = result.fea_at(m_fea_arg.fea_name);
    if (is_output_text) {
        fea.commit_fea(value, sign);
    } else {
        fea.commit_fea_sign(sign);
    }
}

void fea_base::commit_combine_fea(uint32_t fea_sign1, uint32_t fea_sign2, fea_result &result)
{
    uint32_t sign = Hash32(fea_sign1, fea_sign2);
    fea_t &fea = result.fea_at(m_fea_arg.fea_name);
    fea.commit_fea_sign(sign);
}

bool fea_base::check_arg(record &record)
{
    string line = m_fea_arg.dep;
    map <string, uint32_t> &m_pattern = record.m_pattern;
    vector <string> vec;
    util::str_util::trim(line);
    util::str_util::split(line, ",", vec);
    string field_name;

    for (size_t index = 0; index < vec.size(); ++index) {
        field_name = vec[index];
        util::str_util::trim(field_name);

        if (m_pattern.find(field_name) == m_pattern.end()) {
            m_vec_param.clear();
            return false;
        } else {
            m_vec_param.push_back(m_pattern[field_name]);
        }
    }
    return true;
}
}
