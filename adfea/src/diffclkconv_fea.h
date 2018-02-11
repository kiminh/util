/*************************************************************************
PostDictStruct::    > File Name: direct_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:10:09 2014
 ************************************************************************/

#ifndef DIFFCLKCONV_FEA_H
#define DIFFCLKCONV_FEA_H

#include "fea_base.h"

namespace fea {
//fea format:
//fea_name=appid;method=direct_fea;slot=2;
    class diffclkconv_fea : public fea_base {
    public:
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);

    private:
        int pre_click_index;
        int pre_conv_index;
        int cur_click_index;
        int cur_conv_index;

        int click_th;
        int max_trust_click;
    };
}
#endif
