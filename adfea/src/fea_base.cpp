/*************************************************************************
    > File Name: fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一  1/ 5 15:11:09 2015
 ************************************************************************/

#include <map>
#include <iostream>
#include <string.h>
#include "fea_base.h"
#include "str_util.h"

namespace fea {
    using namespace std;

    void fea_base::commit_single_fea(const string &value, fea_result &result) {
		
//		cout << "sign " << sign << " fea_name" << m_fea_arg.fea_name + value << endl;
        uint32_t sign = 0;
        if(result.hash_method == 0){
		    sign = hash_fn(m_fea_arg.fea_name + value);
        }else{
            const char *fea_str = (m_fea_arg.fea_name + value).c_str();

            sign =  CityHash32(fea_str,strlen(fea_str));
        }
		fea_t &fea = result.fea_at(m_fea_arg.fea_name);
        if (is_output_text) {
            fea.commit_fea(value, sign);
        }
        else {
            fea.commit_fea_sign(sign);
        }
    }

    void fea_base::commit_anycombine_fea(vector<vector<uint32_t> > fea_sign_vec,fea_result &result){
        if(fea_sign_vec.size() < 2){
            return;    
        }
        uint32_t sign = 0;
        vector<uint32_t> fvalue0 = fea_sign_vec[0];
        vector<uint32_t> fvalue1 = fea_sign_vec[1];
        for(size_t index0 = 0; index0 < fvalue0.size();++index0){
            for(size_t index1 = 0; index1 < fvalue1.size();++index1){
                sign += Hash32(fvalue0[index0],fvalue1[index1]);
            }
        }
                
        for(size_t i = 2; i < fea_sign_vec.size();++i){
            vector<uint32_t> fvalue = fea_sign_vec[i];
            for(size_t index = 0; index < fvalue.size();++index){
                sign = Hash32(sign,fvalue[index]);
            }
        }
        fea_t &fea = result.fea_at(m_fea_arg.fea_name);
        fea.commit_fea_sign(sign);
    }

    void fea_base::commit_combine_fea(uint32_t fea_sign1, uint32_t fea_sign2, fea_result &result) {
        uint32_t sign = Hash32(fea_sign1, fea_sign2);
        fea_t &fea = result.fea_at(m_fea_arg.fea_name);
        fea.commit_fea_sign(sign);
    }

    bool fea_base::check_arg(record & record) {
        string line = m_fea_arg.dep;
        map<string, uint32_t> &m_pattern = record.m_pattern;
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
            }
            else {
                m_vec_param.push_back(m_pattern[field_name]);
            }
        }
        return true;
    }
}
