/*************************************************************************
    > File Name: extract_main.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 五 12/19 23:01:12 2014
 ************************************************************************/
#include <ctime>
#include <iostream>
#include <fstream>
#include "extractor.h"
#include "sparse_fea.h"
#include "conf_util.h"


void print_featable(std::ofstream& ofs,map<string,uint32_t>& fea_res)
{
	//fea::fea_list& fea_items = fea_res.fea_vec; 
	for(map<string,uint32_t>::iterator iter = fea_res.begin(); iter!=fea_res.end();++iter)
	{
		ofs << iter->first<< "\t" << iter->second << endl;
	}
}


void print_fea(std::ofstream& ofs, fea::instance& fea_res)
{
	ofs << fea_res.label;
	fea::fea_list& fea_items = fea_res.fea_vec; 
	for(uint32_t index = 0; index < fea_items.size(); ++index)
	{
		ofs << " " << fea_items[index].fea_index;
	}
	ofs << "\n";
}

int main(int argc, char* argv[])
{
	using namespace util;
	using namespace std;

    util::conf_util adfea_conf;
    adfea_conf.parse(argv[1]);

	string train_file;
	if(adfea_conf.has_item("train_file"))
	{
		train_file = adfea_conf.getItem<string>("train_file");
	}

	ifstream input_stream(train_file);
	cout << "train_file:" << train_file << endl;

	string train_ins_file;
	if(adfea_conf.has_item("train_instance"))
	{
		train_ins_file = adfea_conf.getItem<string>("train_instance");
	}
	ofstream output_stream(train_ins_file);
	cout << "train_ins_file:" << train_ins_file << endl;
    
	fea::extractor fea_extractor;
	
	if(!fea_extractor.init(argv[1]))
	{
		cout << "extractor init error!" << endl;
		return -1;
	}

	string line;
	fea::instance result;	
	vector<string> vec;

	cout << "begin to adfea...." << endl;
	std::time_t begin_time;
	time(&begin_time);

	map<string,uint32_t> result_text;
	while(getline(input_stream, line))
	{
		vec.clear();
		util::str_util::trim(line);
		util::str_util::split(line, "\001", vec);
		fea_extractor.record_reset();
		if(!fea_extractor.add_record(vec))
		{
			cout << "add record error" << endl;
			cout << line << endl;
			for(size_t i = 0; i < vec.size(); ++i)
			{
				cout << i << " " << vec[i] << " ";
			}
			cout << endl;
			continue;
		}
		fea_extractor.extract_fea();

		fea_extractor.get_featable_result(result_text);
		print_featable(output_stream, result_text);




	}



	fea_extractor.clear();

	std::time_t end_time;
	time(&end_time);
	std::cout << "AdFea cost time: " 
		<< difftime(end_time, begin_time) << "seconds." << std::endl;
	input_stream.close();
	output_stream.close();
}
