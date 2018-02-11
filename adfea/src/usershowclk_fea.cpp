/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "usershowclk_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;
	bool usershowclk_fea::init()
	{
		 if (m_vec_param.size() != 1) {
		        return false;
		    }
		    else{
                m_record_index = m_vec_param[0];
               //arg == 1, extract show
		        if (m_fea_arg.arg == "0") {
		            fea_index = 0;
		        }
                //arg == 2, extract clk
		        else if (m_fea_arg.arg == "1") {
		            fea_index = 1;
		        }
		        else {
		            return false;
		        }
		    }
		    return true;
	}

	bool usershowclk_fea::extract_fea(const record& record, fea_result& result)
	{
		//is_extract = true;
	//	cout << m_fea_arg.fea_name << " " << m_fea_arg.dep << endl;
//		cout << record.valueAt(m_record_index) << endl;
		vector<string> vec;
        //cout << "get ori fea " << record.valueAt(m_record_index) << endl;
		util::str_util::split(record.valueAt(m_record_index), "_", vec);
		if(vec.size() < 2){
			return false;
		}
        string fea = vec[fea_index];

		//cout << fea_index << "pri fea " << record.valueAt(m_record_index) << " getfea " << fea << endl;
		commit_single_fea(fea, result);
		return true;
	}
}
