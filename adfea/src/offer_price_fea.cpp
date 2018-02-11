/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "offer_price_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;

	bool offer_price_fea::init()
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

	bool offer_price_fea::extract_fea(const record& record, fea_result& result)
	{
        float fea = atof(record.valueAt(m_record_index).c_str());
        int price = static_cast<int>(fea * 10);
        if(price > 100){
            price = 100;
        }

        commit_single_fea(to_string(price),result);
        return true;
	}
}
