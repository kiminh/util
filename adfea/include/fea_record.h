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

#include "str_util.h"
using namespace std;

namespace fea
{
	struct record
	{
        bool set_linekey(std::vector<std::string>& tokens,std::vector<uint32_t>& index_vec){
            for(auto iter = index_vec.begin();iter!=index_vec.end();++iter){
                uint32_t index_value = *iter;
                if(index_value > tokens.size()){
                    cerr << "line_no key index oversize tokens!" << endl;
                    return false;
                }
                line_key += tokens[*iter];
            }
            cout << "key_str: " << line_key << endl;
            return true;
        }

		//tokens.size must be equal to field_num
		bool add_record(std::vector<std::string>& tokens)
		{
			if(field_num != tokens.size())
			{
				cout << "WARNING tokens size is not equal to field_num :" << field_num << " " << tokens.size() << endl;
				for(size_t index = 0; index < tokens.size(); ++index)
				{
					cout << "filed index :" << index << " " << tokens[index] << " ";
				}
				return false;
			}

			for(size_t index = 0; index < field_num; ++index)
			{
				m_vec_record[index] = tokens[index];
			}
			return true;
		}

		bool add_record(std::map<std::string, std::string>& field_map)
		{
			return true;
		}

		inline const std::string& valueAt(int index)const 
		{
			return m_vec_record[index];
		}

		void clear()
		{
			std::vector<std::string>::iterator iter = m_vec_record.begin();
			while(iter != m_vec_record.end())
			{
				iter -> clear();
				++iter;
			}
		}

		void parse_schema(std::string& line)
		{
			std::vector<std::string> vec;
			util::str_util::trim(line);
			util::str_util::split(line, ",", vec);
			std::string field_name;

			for(size_t index = 0; index < vec.size(); ++index)
			{
				field_name = vec[index];
				util::str_util::trim(field_name); 
				m_pattern[field_name] = index;
			}
			field_num = vec.size();
			m_vec_record.resize(field_num);
		}
		
		bool contain_field(const std::string& field_name)
		{
			return !(m_pattern.find(field_name) == m_pattern.end());
		}
		size_t field_num;
		std::map<std::string, uint32_t> m_pattern;
		std::vector<std::string> m_vec_record;
        std::string line_key;
	};
}
#endif
