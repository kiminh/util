/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "appname_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;

	appname_fea::appname_fea(string file_name){
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

            
			//uint32_t trade = strtoul(vec[1].c_str(),nullptr,10);
			appname_dict[app] = vec[1];
		}
	}

	bool appname_fea::init()
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

	bool appname_fea::extract_fea(const record& record, fea_result& result)
	{
		//is_extract = true;
//		cout << m_fea_arg.fea_name << " " << m_fea_arg.dep << endl;
        string app(record.valueAt(m_record_index));
		transform(app.begin(),app.end(),app.begin(),::tolower);
		map<string,string>::iterator iter = appname_dict.find(app);
        //cout  << "extract app " << app << endl;
		if(iter!=appname_dict.end()){
			vector<string> seg_vec;
			util::str_util::split(iter->second," ",seg_vec);
            for(size_t i = 0; i < seg_vec.size();i++){
                //cout << "extract app_name " << seg_vec[i] << endl;
			    commit_single_fea(seg_vec[i], result);
            }
			//if(seg_vec.size()<1){
			//	continue;
			//}
            //char tmp[128];
            //snprintf(tmp,128,"%u",iter->second);
		}else{
            
           //cout << "non find app " << app << endl;
        }

		return true;
	}
}
