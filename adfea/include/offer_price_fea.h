/*************************************************************************
PostDictStruct::    > File Name: direct_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:10:09 2014
 ************************************************************************/

#ifndef OFFER_PRICE_FEA_H
#define OFFER_PRICE_FEA_H

#include "fea_base.h"
#include<fstream>
#include <algorithm>

namespace fea {
//fea format:
//fea_name=appid;method=direct_fea;slot=2;
    class offer_price_fea : public fea_base {
    public:

    	//user_interest_fea(std::string);
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);


    private:
        int m_record_index;
    };
}
#endif
