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
    int slot_index;

    std::vector <uint32_t> m_sign;
    bool is_output;

    bool is_extract;

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
        while (iter != m_fea_out.end())
        {
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


    void to_featext_list(std::vector <std::string> &feas)
    {
        feas.clear();
        std::map<std::string, fea_t>::iterator iter = m_fea_out.begin();
        while (iter != m_fea_out.end())
        {
            string fea_name = iter->first;
            //cout << "fea_name " << fea_name << endl;
            std::vector <std::string> m_fea_value = iter->second.m_fea_value;
            bool is_output = iter->second.is_output;
            bool is_extract = iter->second.is_extract;
            if (is_output && is_extract)
            {
                for (size_t i = 0; i < m_fea_value.size(); ++i)
                {
                    //cout << "fea_value " << m_fea_value[i] << endl;
                    feas.push_back(fea_name + ":" + m_fea_value[i]);
                }
            }
            ++iter;
        }
    }

    void to_featable_list(map <string, uint32_t> &fea_map)
    {
        std::map<std::string, fea_t>::iterator iter = m_fea_out.begin();
        while (iter != m_fea_out.end())
        {
            string fea_name = iter->first;

            std::vector <uint32_t> &signs = iter->second.m_sign;
            std::vector <std::string> m_fea_value = iter->second.m_fea_value;


            for (uint32_t i = 0; i < signs.size(); ++i)
            {
                string item_name = fea_name + ":" + m_fea_value[i];
                uint32_t item_sign;

                if (enable_hash)
                {
                    item_sign = signs[i] % hash_num;
                } else
                {
                    item_sign = signs[i];
                }

                fea_map[item_name] = item_sign;
            }

            ++iter;
        }
    }


    void to_fea_list(fea::fea_list &feas)
    {
        feas.clear();
        std::map<std::string, fea_t>::iterator iter = m_fea_out.begin();
        int index = 0;
        while (iter != m_fea_out.end())
        {
            string fea_name = iter->first;
            bool is_output = iter->second.is_output;
            std::vector <uint32_t> &signs = iter->second.m_sign;
            bool is_extract = iter->second.is_extract;

            if (is_output && is_extract)
            {
                for (uint32_t i = 0; i < signs.size(); ++i)
                {
                    fea_item item;
                    item.slot_index = iter->second.slot_index;
                    if (enable_hash)
                    {
                        item.fea_index = signs[i] % hash_num;
                    }
                    else
                    {
                        item.fea_index = signs[i];
                    }
                    //feas.push_back(item);
                    item.fea_name = fea_name;
                    feas.push_back(item);
                }
            }
            ++iter;
            index += 1;
        }
    }


    void get_belta0(uint32_t &belta0)
    {
        //cout << "fdsfds " << endl;
        //if(is_belta0){
        //cout << "cout belta0 " << endl;
        for (std::map<std::string, fea_t>::iterator iter = m_fea_out.begin();
             iter != m_fea_out.end(); ++iter)
        {

            std::vector <uint32_t> &signs = iter->second.m_sign;
            //cout << "iter->fea_name " << iter->first << endl;
            if (iter->first.find("beta"))
            {
                for (uint32_t i = 0; i < signs.size(); ++i)
                {
                    cout << "pass belta0 " << i << "\t" << signs[i] << "\t" << signs[i] % hash_num << endl;
                    if (enable_hash)
                    {
                        belta0 = signs[i] % hash_num;
                    }
                    else
                    {
                        belta0 = signs[i];
                    }
                }
                break;

            }
        }
    }

    void dump_fea(std::map <std::string, uint32_t> &fea_dict)
    {
        std::map<std::string, fea_t>::iterator iter = m_fea_out.begin();
        while (iter != m_fea_out.end())
        {
            string fea_name = iter->first;
            std::vector <std::string> m_fea_value = iter->second.m_fea_value;
            std::vector <uint32_t> &signs = iter->second.m_sign;
            //cout << "fea_name: " << fea_name << " fea_value:size: " << m_fea_value.size() << endl;
            if (signs.size() < m_fea_value.size())
            {

                //cerr << "fea_sign size:"<< signs.size() << " is not equal to fea_text:" << m_fea_value.size() << endl;
                return;
            }
            for (size_t i = 0; i < m_fea_value.size(); ++i)
            {
                //feas.push_back(fea_name + ":" + m_fea_value[i]);
                uint32_t fea_id = signs[i];
                if (enable_hash)
                {
                    fea_id = signs[i] % hash_num;
                }
                //cout << "fea_value: " << fea_name << ":" << m_fea_value[i] << endl;
                //cout << "fea_id:" << fea_id << endl;

                fea_dict[fea_name + ":" + m_fea_value[i]] = fea_id;
            }
            ++iter;
        }
    }

    int label;
    bool enable_hash;
    uint32_t hash_num;
    int hash_method;
};
}
#endif