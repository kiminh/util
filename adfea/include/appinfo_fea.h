/*************************************************************************
PostDictStruct::    > File Name: direct_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:10:09 2014
 ************************************************************************/

#ifndef APPINFO_FEA_H
#define APPINFO_FEA_H

#include "fea_base.h"
#include<fstream>
#include <algorithm>
#include <unordered_map>

namespace fea {
//fea format:
//fea_name=appid;method=direct_fea;slot=2;
    class appinfo_fea : public fea_base {
    public:

    	appinfo_fea(std::string);
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);


    private:
        int m_record_index;
        uint32_t field_index;
        std::unordered_map<std::string,std::string> apprank_dict;
        std::unordered_map<std::string,std::string> apptrade_dict;
        std::unordered_map<std::string,std::string> appseller_dict;

        std::vector<std::unordered_map<std::string,std::string> > dict_vec;
    };
}
#endif
