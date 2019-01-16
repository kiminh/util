/*************************************************************************
    > File Name: location_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: Mon 20 Apr 2015 03:23:07 PM CST
 ************************************************************************/

#ifndef LOCATION_FEA_H
#define LOCATION_FEA_H

#include "fea_base.h"

namespace fea
{
//lr_fea format:
//fea_name=appid;method=direct_fea;slot=2;
class location_fea
    :
        public fea_base
{
public:
    virtual bool init();
    virtual bool extract_fea(const record &record, fea_result &result);
private:
    int location_index;
    int fea_index;
};
}
#endif
