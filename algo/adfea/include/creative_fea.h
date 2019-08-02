/*************************************************************************
PostDictStruct::    > File Name: direct_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:10:09 2014
 ************************************************************************/

#ifndef CREATIVE_FEA_H
#define CREATIVE_FEA_H

#include "fea_base.h"
#include<fstream>

namespace fea {
//fea format:
//fea_name=appid;method=direct_fea;slot=2;
    class creative_fea : public fea_base {
    public:

    	creative_fea(std::string,std::string);
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);


    private:
        int m_record_index;
        std::map<int,uint> creative_dict;
        string creative_dft;
    };
}
#endif
