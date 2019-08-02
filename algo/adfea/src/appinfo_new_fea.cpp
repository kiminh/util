/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "appinfo_new_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;

	bool appinfo_new_fea::init()
	{
		if(m_vec_param.size() != 1)
		{
			return false;
		}
		else
		{
			m_record_index = m_vec_param[0];
		}
		field_index = strtoul(m_fea_arg.arg.c_str(),nullptr,10);
		return true;	
	}

	bool appinfo_new_fea::extract_fea(const record& record, fea_result& result)
	{
		//is_extract = true;
        vector<string> vec;
        util::str_util::split(record.valueAt(m_record_index),"|",vec);

        if(field_index >= vec.size()){
            return false;    
        }
    
		commit_single_fea(vec[field_index], result);

		return true;
	}
}
