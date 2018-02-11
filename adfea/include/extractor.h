/*************************************************************************
    > File Name: extractor.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 五 12/19 23:08:28 2014
 ************************************************************************/
#ifndef EXTRACTOR_H
#define EXTRACTOR_H

#include <string>
#include <vector>
#include <map>

#include "sparse_fea.h"
#include "fea_record.h"
#include "fea_base.h"
#include "sparse_fea.h"
#include "str_util.h"


namespace fea {
	struct PostDictStruct{

		bool init(){
			return true;
		}
		bool load_app(std::string);
		bool load_creative(std::string);
		std::map<std::string,uint> app_dict;
		std::map<int,uint> creative_dict;
		std::string app_def;
		std::string creative_def;

	};


	typedef struct PostDictStruct PostDict;

    class extractor {
    public:
        extractor() {
            enable_hash = false;
            hash_num = 0;
        }

        bool init(const char *conf);

        void clear();

    public:
        void record_reset();

        bool add_record(std::map<std::string, std::string> &field_map);

        bool add_record(std::vector<std::string> &field);

        //online and offline use same api
        bool extract_fea();

        void get_fea_result(fea::instance &result);

        void get_fea_list(fea::fea_list &list);

        void get_featext_list(std::vector<std::string>&,int&);
		void get_featext_result(vector<string>&, int &);
		void get_belta0(uint32_t &belta0);
		void get_featable_result(map<string,uint32_t>&);
        void dump_fea(std::map<std::string,uint32_t>&);

    private:
        bool init_fea_list(std::string &fea_conf, std::string &pattern);

        void init_result();

        fea_base *get_fea_method(std::string &fea_name);

    private:
        bool enable_hash;
        bool on_line;
        uint32_t hash_num;
        fea_result result;
        int32_t label_index;
        record m_record;
        std::vector<fea_base *> vec_fea_list;

        int need_cluster;
        //PostDict* postDictsPtr;
        string app_dict_filename;
        string creative_dict_filename;
        string app_dft;
        string creative_dft;

        string apptrade_filename;
        string appname_filename;
        string appinfo_filename;

        vector<uint32_t> line_key_vec;

        bool line_flag;

        int hash_method;


    };
}
#endif
