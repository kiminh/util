/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "offerclkconv_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;
	bool offerclkconv_fea::init()
	{
		 if (m_vec_param.size() != 2) {
		        return false;
		    }
         
         click_index = m_vec_param[0];
         conv_index = m_vec_param[1];

         click_th = static_cast<int>(atoi(m_fea_arg.arg.c_str()));
         //cout << " click_th : "<< click_th << endl;
		 return true;
	}

	bool offerclkconv_fea::extract_fea(const record& record, fea_result& result)
	{
        string fea;

        int click_num = 0;
        if(!record.valueAt(click_index).empty()){
            click_num = static_cast<int>(atoi(record.valueAt(click_index).c_str()));
        }else{
            is_extract = false;
            return false;
        }

        int conv_num = 0;
        if(!record.valueAt(conv_index).empty()){
            conv_num = static_cast<int>(atoi(record.valueAt(conv_index).c_str()));
        }else{
            is_extract = false;
            return false;
        }

        if ( click_num > click_th && click_th != 0 ) {
            conv_num = int ((float)conv_num / click_num * click_th);
            fea = to_string(click_th) + "_" + to_string(conv_num);
        }else{
            fea = to_string(click_num) + "_" + to_string(conv_num);
        }
        //cout << fea << endl;
		commit_single_fea(fea, result);
        is_extract = true;
		return true;
	}
}
