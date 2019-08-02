/*************************************************************************
    > File Name: combined_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 三 12/31 16:27:12 2014
 ************************************************************************/

#ifndef COMBINED_FEA_H
#define COMBINED_FEA_H

#include <string>

#include "fea_base.h"

namespace fea {
    class combined_fea : public fea_base {
    public:
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);

    private:
        std::string fea_name1;
        std::string fea_name2;
    };
}

#endif
