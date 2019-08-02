/*************************************************************************
    > File Name: direct_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 一 12/22 18:18:11 2014
 ************************************************************************/

#include "creative_fea.h"
#include "str_util.h"
#include <iostream>

namespace fea
{
	using namespace std;
	using namespace util;

	creative_fea::creative_fea(string file_name,string dft){

		//ifstream model_file(file_name);
		//ifstream app_file(file_name);
		//ifstream model_file(filename);
		ifstream fea_stream(file_name);
		string line;
		while(getline(fea_stream,line)){
			vector<string> vec;
			util::str_util::split(line,"\t",vec);
			if(vec.size()<2){
				continue;
			}
			int creative = atoi(vec[0].c_str());
			uint show = strtoul(vec[1].c_str(),nullptr,10);
			creative_dict[creative] = show;
		}
		creative_dft = dft;

	}
	bool creative_fea::init()
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

	bool creative_fea::extract_fea(const record& record, fea_result& result)
	{
		//is_extract = true;
//		cout << m_fea_arg.fea_name << " " << m_fea_arg.dep << endl;
//
		int creative = atoi(record.valueAt(m_record_index).c_str());
		map<int,uint>::iterator iter = creative_dict.find(creative);
		if(iter!=creative_dict.end()){
            cout << "find creative " << endl;
			commit_single_fea(record.valueAt(m_record_index), result);
		}else{
            cout << "miss creative " << creative_dft << endl;
			commit_single_fea(creative_dft, result);
		}

		return true;
//		cout << record.valueAt(m_record_index) << endl;
		commit_single_fea(record.valueAt(m_record_index), result);
		return true;
	}
}
