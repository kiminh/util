/*************************************************************************
PostDictStruct::    > File Name: direct_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:10:09 2014
 ************************************************************************/

#ifndef OFFERCLKCONV_FEA_H
#define OFFERCLKCONV_FEA_H

#include "fea_base.h"

namespace fea {
//fea format:
//fea_name=appid;method=direct_fea;slot=2;
    class offerclkconv_fea : public fea_base {
    public:
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);

    private:
        int click_index;
        int conv_index;
        int click_th;
    };
}
#endif
