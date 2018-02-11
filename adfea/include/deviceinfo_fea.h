/*************************************************************************
PostDictStruct::    > File Name: direct_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:10:09 2014
 ************************************************************************/

#ifndef DEVICEINFO_FEA_H
#define DEVICEINFO_FEA_H

#include "fea_base.h"

namespace fea {
//fea format:
//fea_name=appid;method=direct_fea;slot=2;
    class deviceinfo_fea : public fea_base {
    public:
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);

    private:
        int feild_index;

        //cooee_adspot_id,cooee_make,install_authority
        std::string key;
    };
}
#endif
