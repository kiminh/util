/*************************************************************************
    > File Name: beta.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 五 12/19 22:53:40 2014
 ************************************************************************/
#ifndef BETA_H
#define BETA_H

#include "fea_base.h"

namespace fea
{
class beta
    :
        public fea_base
{
public:
    virtual bool init();

    virtual bool extract_fea(const record &record, fea_result &result);
};
}
#endif
