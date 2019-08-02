/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "appid_fea.h"
#include "str_util.h"
#include <iostream>
#include <cctype>

#include <iterator>
#include <algorithm>

namespace fea
{
	using namespace std;
	using namespace util;

	appid_fea::appid_fea(string file_name,string dft){

		//ifstream model_file(file_name);
		//ifstream app_file(file_name);
		//ifstream model_file(filename);
        cout << "file " << file_name << endl;
        cout << "dft " << dft << endl;
		ifstream fea_stream(file_name);
		string line;
		while(getline(fea_stream,line)){
			vector<string> vec;
			util::str_util::split(line,"\t",vec);
			if(vec.size()<2){
                cout << "format error " << line << endl;
				continue;
			}
			string app = vec[0];
			transform(app.begin(),app.end(),app.begin(),::tolower);
			uint show = strtoul(vec[1].c_str(),nullptr,10);
            cout << "add into vec " << app << show << endl;
			app_dict[app] = show;
		}
		app_dft = dft;

	}
	bool appid_fea::init()
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

	bool appid_fea::extract_fea(const record& record, fea_result& result)
	{
		string app = record.valueAt(m_record_index);
		transform(app.begin(),app.end(),app.begin(),::tolower);
        cout << "find app_id " << app << endl;
		map<string,uint>::iterator iter = app_dict.find(app);
		if(iter!=app_dict.end()){
            cout << "appid " << app << " found! extract "  << endl;
			commit_single_fea(record.valueAt(m_record_index), result);
		}else{
            cout << "appid " << app << " unfound! extract emit "  <<  app_dft <<endl;
			commit_single_fea(app_dft, result);
		}

		return true;
	}
}
