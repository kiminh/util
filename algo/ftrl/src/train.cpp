/*************************************************************************
    > File Name: train.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 二 12/16 20:19:24 2014
 ************************************************************************/

#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <ctime>
#include "ftrl.h"
#include "str_util.h"
#include "conf_util.h"

void print_help()
{
	std::cout <<"Usage:" 
		<< "train fea_num run.conf" << std::endl;
	exit(0);
}

int main(int argc, char* argv[])
{
	using namespace std;

	if (argc < 3 || !strcmp(argv[1], "-help") || !strcmp(argv[1], "--help") ||
		!strcmp(argv[1], "-h") || !strcmp(argv[1], "-usage")) 
	{
		print_help();
	}

	util::str_util m_str_util;
	int fea_num = m_str_util.castFromS<int>(argv[1]);

	if(fea_num <= 0)
	{
		std::cout << "fea_num seting is not correct, must be positive" << std::endl;
		exit(1);
	}
	
	string conf_file(argv[2]);
	util::conf_util ftrl_conf;
	ftrl_conf.parse(conf_file);

	int pass = 1;
	if(argc == 4)
	{
		int temp_pass = m_str_util.castFromS<int>(argv[3]);
		pass = temp_pass > 0 ? temp_pass : 1;
	}

    bool is_incre = false;
    if(ftrl_conf.has_item("is_incre")){
        is_incre = ftrl_conf.getItem<bool>("is_incre");
    }

    bool save_aux = true;
    if(ftrl_conf.has_item("save_aux")){
        save_aux = ftrl_conf.getItem<bool>("save_aux");
    }

	string train_file = ftrl_conf.getItem<string>("train_file");
	string model_file = ftrl_conf.getItem<string>("model_file");
	double alpha = ftrl_conf.getItem<double>(string("alpha"));
	double beta = ftrl_conf.getItem<double>(string("beta"));
	double l1reg = ftrl_conf.getItem<double>(string("l1reg"));
	double l2reg = ftrl_conf.getItem<double>(string("l2reg"));

	string memory_in("stream");
	if(ftrl_conf.has_item("memory_in"))
		memory_in = ftrl_conf.getItem<string>("memory_in");

	string old_model_file("");
	if(is_incre){
		if(!ftrl_conf.has_item("old_model_file")){
			cerr << "no old batch model exist! " << endl;
			return -1;
		}
		old_model_file = ftrl_conf.getItem<string>("old_model_file"); 
	}

	algo::Ftrl ftrl_model(alpha, beta, l1reg, l2reg, fea_num);
	cout << pass << endl;

	string line;
	fea::instance ins;

	std::time_t begin_time;
	time(&begin_time);
	struct tm * timeinfo;
	timeinfo = localtime (&begin_time);
	cout << "Begin to train model at time ...." << endl;
	cout << asctime(timeinfo) << endl;

    if(is_incre){
        ftrl_model.load_model(old_model_file);
        cout << "finish load batch model" << endl;
    }

	if(memory_in == "stream") {
		for(int index = 0; index < pass; ++index)
		{
			std::ifstream train_stream(train_file);
			int total_ins = 0;
			while(getline(train_stream, line))
			{
				ins.reset();
				++total_ins;
				if(total_ins % 1000000 == 0)
				{
					std::cout << "train instances :" << total_ins << std::endl;
				}

				ftrl_model.parse_line(line, ins);
				ftrl_model.train_instance(ins);
			}
			train_stream.close();
			cout << "Train pass: " << index << endl;
			cout << "Total instance is : "<< total_ins << endl;
		}
	}else if(memory_in == "batch"){
		std::ifstream train_stream(train_file);
		ftrl_model.load_train_data(train_stream);
		ftrl_model.random_shuffle();
		ftrl_model.train_batch();
	}else{
		std::cerr << "error memory_in type!!!" << std::endl;
	}

	ftrl_model.save_model(model_file,save_aux);
	std::time_t end_time;
	time(&end_time);
	std::cout << "Total train time is : " << 
	difftime(end_time, begin_time) << "seconds." << std::endl;
}
