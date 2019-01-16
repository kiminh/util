/*************************************************************************
    > File Name: fea_record.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 三 12/31 00:16:06 2014
 ************************************************************************/

#ifndef FEA_RECORD_H
#define FEA_RECORD_H

#include <string>
#include <vector>
#include <map>
#include <iostream>
#include "bidmax/common/str_util.h"

using namespace std;

namespace fea
{
struct record
{
    bool add_record(std::vector <std::string> &tokens)
    {
        //token.size 和 field_num不相同的时候支持兼容
        size_t valid_field_num = field_num;
        if (tokens.size() > field_num) {
            cerr << "token.size is larger than field_num!" << endl;
        } else if (tokens.size() < field_num) {
            valid_field_num = tokens.size();
            cerr << "token.size is less than field_num!" << endl;
        }

        for (size_t index = 0; index < valid_field_num; ++index) {
            m_vec_record[index] = tokens[index];
        }
        if (valid_field_num < m_vec_record.size()) {
            for (size_t index = valid_field_num; index < m_vec_record.size(); ++index) {
                m_vec_record[index] = "";
            }
        }
        return true;
    }

    bool add_record(std::map <std::string, std::string> &field_map)
    {
        return true;
    }

    inline const std::string &valueAt(int index) const
    {
        return m_vec_record[index];
    }

    void clear()
    {
        std::vector<std::string>::iterator iter = m_vec_record.begin();
        while (iter != m_vec_record.end()) {
            iter->clear();
            ++iter;
        }
    }

    void parse_schema(std::string &line)
    {
        std::vector <std::string> vec;
        util::str_util::trim(line);
        util::str_util::split(line, ",", vec);
        std::string field_name;

        for (size_t index = 0; index < vec.size(); ++index) {
            field_name = vec[index];
            util::str_util::trim(field_name);
            m_pattern[field_name] = index;
        }
        field_num = vec.size();
        m_vec_record.resize(field_num);
    }

    bool contain_field(const std::string &field_name)
    {
        return !(m_pattern.find(field_name) == m_pattern.end());
    }

    size_t field_num;
    std::map <std::string, uint32_t> m_pattern;
    std::vector <std::string> m_vec_record;
};
}
#endif
