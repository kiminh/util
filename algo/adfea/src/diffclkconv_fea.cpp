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
        if (m_vec_param.size() != 4) {
            return false;
        }

        pre_click_index = m_vec_param[0];
        pre_conv_index  = m_vec_param[1];
        cur_click_index = m_vec_param[2];
        cur_conv_index  = m_vec_param[3];

        max_trust_click1 = 80;
        max_trust_click2 = 30;
        click_th = 200;

        vector<string> param_vec;
        str_util::split(m_fea_arg.arg,"_",param_vec);

        max_trust_click1 = static_cast<int>(atoi(param_vec[0].c_str()));
        max_trust_click2 = static_cast<int>(atoi(param_vec[1].c_str()));
        click_th         = static_cast<int>(atoi(param_vec[2].c_str()));
        std::cout << max_trust_click1 << " " << max_trust_click2 << std::endl;

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

        int cur_click = 0;
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

        int click = pre_click + cur_click;
        int conv = pre_conv + cur_conv;

        if(click > max_trust_click1 && conv == 0)
        {
            fea = "0_" + to_string(click_th) + "_0";
        }else if(click < max_trust_click1 && conv != 0){
            fea = "1_0_1";
        }else if(click < max_trust_click1 && conv == 0){
            fea = "0_0_0";
        }else{
            int c1 = static_cast<int>(conv * 1.0/click * click_th);
            int diffclk = cur_click - pre_click;

            if(cur_click < max_trust_click2 || pre_click < max_trust_click2)
                fea = to_string(c1) + "_" + to_string(click_th) + "_x";
            else{
                if(diffclk == 0)
                    fea = to_string(c1) + "_" + to_string(click_th) + "_0";
                else{
                    float pcvr_c1h = cur_conv * 1.0 / cur_click;
                    float pcvr_p1h = pre_conv * 1.0 / pre_click;

                    int c2 = static_cast<int>((pcvr_c1h - pcvr_p1h) * max_trust_click2);
                    fea = to_string(c1) + "_" + to_string(click_th) + "_" + to_string(c2);
                }
            }
        }
//        std::cout << fea << std::endl;
        commit_single_fea(fea, result);
        is_extract = true;
        return true;
    }
}
