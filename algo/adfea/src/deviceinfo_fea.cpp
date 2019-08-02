/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "deviceinfo_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;
	bool deviceinfo_fea::init()
	{
		 if (m_vec_param.size() != 1) {
		        return false;
		    }

        feild_index = m_vec_param[0];
        key = m_fea_arg.arg;

		return true;
	}

	bool deviceinfo_fea::extract_fea(const record& record, fea_result& result)
	{
        vector<string> vec;
        string fea;

        str_util::split(record.valueAt(feild_index),"\002",vec);

        if(vec.size() < 3) {
            std::cerr << "origin feature data error!" << std::endl;
            is_extract = false;
            return false;
        }
        if(key == "install_authority") {
            fea = vec[0];
        }else if (key == "cooee_make") {
            fea = vec[1];
        }else if (key == "cooee_adspot_id") {
            fea = vec[2];
        }else{
            std::cerr << "unspecified args name!" << std::endl;
            is_extract = false;
            return false;
        }

        commit_single_fea(fea, result);
        is_extract = true;
		return true;
	}
}
