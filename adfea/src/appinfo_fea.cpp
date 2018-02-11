/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "appinfo_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;

	appinfo_fea::appinfo_fea(string file_name){
		ifstream fea_stream(file_name);
		string line;
		while(getline(fea_stream,line)){

			transform(line.begin(),line.end(),line.begin(),::tolower);
			vector<string> vec;
			util::str_util::split(line,"\t",vec);
			if(vec.size()<7){
				continue;
			}
			string app(vec[0]);
            string rank(vec[6]);           
            string seller(vec[5]);
            string trade(vec[1]);

			apptrade_dict[app] = trade;
			appseller_dict[app] = seller;
            apprank_dict[app] = rank;

		}
        dict_vec.push_back(apptrade_dict);
        dict_vec.push_back(appseller_dict);
        dict_vec.push_back(apprank_dict);
	}

	bool appinfo_fea::init()
	{
		if(m_vec_param.size() != 1)
		{
			return false;
		}
		else
		{
			m_record_index = m_vec_param[0];
		}
		field_index = strtoul(m_fea_arg.arg.c_str(),nullptr,10);
		return true;	
	}

	bool appinfo_fea::extract_fea(const record& record, fea_result& result)
	{
		//is_extract = true;
        string app(record.valueAt(m_record_index));
		transform(app.begin(),app.end(),app.begin(),::tolower);

        if(field_index >= dict_vec.size()){
            return false;    
        }  
		unordered_map<string,string> dict = dict_vec[field_index];

		unordered_map<string,string>::iterator iter = dict.find(app);
		if(iter!=dict.end()){
            //cout << "find app " << record.valueAt(m_record_index) << " para:" << field_index << " info " << iter->second << endl;
			commit_single_fea(iter->second, result);
		}else{
            
			commit_single_fea("", result);
           //cout << "non find app " << app << endl;
        }

		return true;
	}
}
