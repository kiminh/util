/*************************************************************************
    > File Name: beta.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 四  1/ 1 13:30:56 2015
 ************************************************************************/
#include "beta.h"

namespace fea {
    bool beta::init() {
        return true;
    }

    bool beta::extract_fea(const record &record, fea_result &result) {
        commit_single_fea("beta", result);
        return true;
    }
}
