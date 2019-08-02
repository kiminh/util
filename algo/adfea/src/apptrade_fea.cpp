/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "apptrade_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;

	apptrade_fea::apptrade_fea(string file_name){
		ifstream fea_stream(file_name);
		string line;
		while(getline(fea_stream,line)){
			vector<string> vec;
			util::str_util::split(line,"\t",vec);
			if(vec.size()<2){
				continue;
			}
			string app(vec[0]);
			transform(app.begin(),app.end(),app.begin(),::tolower);
			uint32_t trade = strtoul(vec[1].c_str(),nullptr,10);
			apptrade_dict[app] = trade;
		}
	}

	bool apptrade_fea::init()
	{
		if(m_vec_param.size() != 1)
		{
			return false;
		}
		else
		{
			m_record_index = m_vec_param[0];
		}
		return true;	
	}

	bool apptrade_fea::extract_fea(const record& record, fea_result& result)
	{
		//is_extract = true;
//		cout << m_fea_arg.fea_name << " " << m_fea_arg.dep << endl;
        string app(record.valueAt(m_record_index));
		transform(app.begin(),app.end(),app.begin(),::tolower);
		map<string,uint32_t>::iterator iter = apptrade_dict.find(app);
		if(iter!=apptrade_dict.end()){
            //cout << "find app " << record.valueAt(m_record_index) << "trade " << iter->second << endl;
            //cout << "find app " << app << "trade " << iter->second << endl;
            char tmp[128];
            snprintf(tmp,128,"%u",iter->second);
			commit_single_fea(string(tmp), result);
		}else{
            
           //cout << "non find app " << app << endl;
        }

		return true;
	}
}
