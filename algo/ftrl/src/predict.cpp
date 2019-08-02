/*************************************************************************
    > File Name: predict.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 二 12/16 20:19:24 2014
 ************************************************************************/

#include <iostream>
#include <string>
#include <fstream>
#include <ctime>
#include "ftrl.h"
#include "../include/str_util.h"
#include "../include/conf_util.h"

void print_help()
{
	std::cout <<"Usage:" 
		<< "predict run.conf" << std::endl;
	exit(0);
}

int main(int argc, char* argv[])
{
	using namespace std;

	if(argc != 2)
	{
		print_help();
	}

	string conf_file(argv[1]);
	util::conf_util ftrl_conf;
	ftrl_conf.parse(conf_file);
	
	string valid_file = ftrl_conf.getItem<string>("valid_file");
	string model_file = ftrl_conf.getItem<string>("model_file");
	string predict_file_out = ftrl_conf.getItem<string>("predict_out");

	algo::Ftrl ftrl_model;
	ftrl_model.load_model(model_file);	
	ftrl_model.load_static_model(model_file);
	std::time_t begin_time;
	time(&begin_time);
	
	ofstream out_file(predict_file_out);

	//begin predict
	std::ifstream predict_stream(valid_file);
	string line;
	fea::instance ins;
	int total_num = 0;

	std::cout << "Begin to predict ....." << std::endl;
	while(getline(predict_stream, line))
	{
		++total_num;
		if(total_num % 100000 == 0)
		{
			cout << "predict lines: " <<  total_num << endl;
		}
		ins.reset();
		ftrl_model.parse_line(line, ins);
//		double score = ftrl_model.predict_instance(ins);
		double score = ftrl_model.predict_instance_static(ins);
		out_file << score << " " << line << endl;
	}
	
	predict_stream.close();
	out_file.close();
	std::time_t end_time;
	time(&end_time);
	std::cout << "Total predict time is : " << 
		difftime(end_time, begin_time) << "seconds." << std::endl;
}
