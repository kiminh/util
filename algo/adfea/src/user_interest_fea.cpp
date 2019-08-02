/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "user_interest_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;

	bool user_interest_fea::init()
	{
		if(m_vec_param.size() != 1)
		{
			return false;
		}
		else
		{
			m_record_index = m_vec_param[0];
		}
		return true;	
	}

	bool user_interest_fea::extract_fea(const record& record, fea_result& result)
	{
		vector<string> vec;
		util::str_util::split(record.valueAt(m_record_index), " ", vec);
		if(vec.size() < 1){
			return false;
		}
		string fea = vec[0];
		for(uint i = 0; i < vec.size();i++){
            cout << "feature " << vec[i] << endl;
		    commit_single_fea(vec[i], result);
		}
		return true;
	}
}
