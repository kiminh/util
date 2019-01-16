/*************************************************************************
    > File Name: fea_result.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 六 12/27 23:51:30 2014
 ************************************************************************/

#ifndef FEA_RESULT_H
#define FEA_RESULT_H

#include <string>
#include <map>
#include "sparse_fea.h"
#include <iostream>

using namespace std;

namespace fea
{
typedef struct _fea_t
{
    std::vector <std::string> m_fea_value;
    std::string slot;

    std::vector <uint32_t> m_sign;
    bool is_output;

    void clear()
    {
        m_fea_value.clear();
        m_sign.clear();
    }

    void commit_fea(const std::string &fvalue, uint32_t fsign)
    {
        m_fea_value.push_back(fvalue);
        m_sign.push_back(fsign);
    }

    void commit_fea_sign(uint32_t fsign)
    {
        m_sign.push_back(fsign);
    }
} fea_t;

struct fea_result
{
    std::map <std::string, fea_t> m_fea_out;

    void clear()
    {
        std::map<std::string, fea_t>::iterator iter = m_fea_out.begin();
        while (iter != m_fea_out.end()) {
            iter->second.clear();
            ++iter;
        }
    }

    fea_t &fea_at(std::string &fea_name)
    {
        return m_fea_out[fea_name];
    }

    void put_fea(std::string fea_name, fea_t fea_value)
    {
        m_fea_out[fea_name] = fea_value;
    }

    const std::vector <uint32_t> &get_fea_value(std::string &fea_name)
    {
        return m_fea_out[fea_name].m_sign;
    }

    void to_fea_list(fea::fea_list &feas)
    {
        feas.clear();
        std::map<std::string, fea_t>::iterator iter = m_fea_out.begin();
        while (iter != m_fea_out.end()) {
            bool is_output = iter->second.is_output;
            std::vector <uint32_t> &signs = iter->second.m_sign;
            if (is_output) {
                for (uint32_t i = 0; i < signs.size(); ++i) {
                    if (enable_hash) {
                        feas.push_back(signs[i] % hash_num);
                    } else {
                        feas.push_back(signs[i]);
                    }
                }
            }
            //	if(signs.size() >0)
            //		cout << "name :" << iter->first << " value:" << signs[0] << " " << signs[0] % hash_num << endl;
            //	else
            //		cout << "name :" << iter->first << endl;
            ++iter;
        }
    }

    int label;
    bool enable_hash;
    uint32_t hash_num;
};
}
#endif
