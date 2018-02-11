/*************************************************************************
    > File Name: device_id_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: Mon 20 Apr 2015 03:23:07 PM CST
 ************************************************************************/

#ifndef DEVICE_ID_FEA_H
#define DEVICE_ID_FEA_H

#include "fea_base.h"

//dep=didsha1,didmd5
namespace fea {
//fea format:
//fea_name=appid;method=direct_fea;slot=2;
    class device_id_fea : public fea_base {
    public:
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);

    private:
        int sha1_index;
        int md5_index;
    };
}
#endif
