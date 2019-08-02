/*************************************************************************
PostDictStruct::    > File Name: direct_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:10:09 2014
 ************************************************************************/

#ifndef DIFFCLKCONV_FEA_V2_H
#define DIFFCLKCONV_FEA_V2_H

#include "fea_base.h"

namespace fea {
//fea format:
//fea_name=appid;method=direct_fea;slot=2;
    class diffclkconv_fea_v2 : public fea_base {
    public:
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);

    private:
        int pre_click_index;
        int pre_conv_index;
        int cur_click_index;
        int cur_conv_index;

        int click_th1;
        int click_th2;
        int max_trust_click1;
        int max_trust_click2;
    };
}
#endif
