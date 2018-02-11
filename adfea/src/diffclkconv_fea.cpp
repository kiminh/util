/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "diffclkconv_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;
	bool diffclkconv_fea::init()
	{
		 if (m_vec_param.size() != 2) {
		        return false;
		    }
         
         pre_click_index = m_vec_param[0];
         pre_conv_index  = m_vec_param[1];
//         cur_click_index = m_vec_param[2];
//         cur_conv_index  = m_vec_param[3];

         max_trust_click = 20;
         click_th = 200;
         vector<string> param_vec;
         str_util::split(m_fea_arg.arg,"_",param_vec);
        
         max_trust_click = static_cast<int>(atoi(param_vec[0].c_str()));
         click_th        = static_cast<int>(atoi(param_vec[1].c_str()));
         std::cout << max_trust_click << std::endl;
		 return true;
	}

	bool diffclkconv_fea::extract_fea(const record& record, fea_result& result)
	{
        string fea;

        int pre_click = 0;
        if(!record.valueAt(pre_click_index).empty()){
            pre_click = static_cast<int>(atoi(record.valueAt(pre_click_index).c_str()));
        }else{
            is_extract = false;
            return false;
        }

        int pre_conv = 0;
        if(!record.valueAt(pre_conv_index).empty()){
            pre_conv = static_cast<int>(atoi(record.valueAt(pre_conv_index).c_str()));
        }else{
            is_extract = false;
            return false;
        } 
        
/*        int cur_click = 0;
        if(!record.valueAt(cur_click_index).empty()){
            cur_click = static_cast<int>(atoi(record.valueAt(cur_click_index).c_str()));
        }else{
            is_extract = false;
            return false;
        }

        int cur_conv = 0;
        if(!record.valueAt(cur_conv_index).empty()){
            cur_conv = static_cast<int>(atoi(record.valueAt(cur_conv_index).c_str()));
        }else{
            is_extract = false;
            return false;
        } 
*/
//        std::cout << pre_click << " " << pre_conv << " " << cur_click << " " << cur_conv << endl;
/*        int click = click_th;
        float diff_cvr = 0.0f;
        if(cur_click < max_trust_click || pre_click == 0){
            click = 0;
        }else{
            float cur_cvr = static_cast<float>(cur_conv) / cur_click;
            float pre_cvr = static_cast<float>(pre_conv) / pre_click;
                
            if(cur_cvr >= 1)
                cur_cvr = 1;
            if(pre_conv < 0 && pre_click < 0)
                pre_cvr = 0;
            
            diff_cvr = cur_cvr - pre_cvr;
        }
*/
        int click = click_th;
        float diff_cvr = 0.0f;
//        std::cout << pre_click << pre_conv << max_trust_click << std::endl;
        if(pre_click < max_trust_click)
            click = 0;
        else
            diff_cvr = static_cast<float>(pre_conv) / pre_click;

        int conversion = static_cast<int>(click * diff_cvr);
        fea = to_string(conversion) + "_" + to_string(click);
//        std::cout << fea << std::endl;
        commit_single_fea(fea, result);
        is_extract = true;
        return true;
    }
}
