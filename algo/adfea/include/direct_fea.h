/*************************************************************************
    > File Name: direct_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:10:09 2014
 ************************************************************************/

#ifndef DIRECT_FEA_H
#define DIRECT_FEA_H

#include "fea_base.h"
#include <algorithm>

namespace fea {
//fea format:
//fea_name=appid;method=direct_fea;slot=2;
    class direct_fea : public fea_base {
    public:
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);

    private:
        int m_record_index;
    };
}
#endif
