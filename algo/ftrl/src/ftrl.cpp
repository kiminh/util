/*************************************************************************
    > File Name: Ftrl.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 二 12/16 16:00:06 2014
 ************************************************************************/
#include <fstream>
#include <cmath>
#include <cstdlib>
#include "ftrl.h"
#include <iostream>
#include <algorithm>

namespace algo
{
using namespace std;
using namespace fea;
Ftrl::Ftrl(double alpha, double beta, double l1reg, double l2reg, uint32_t fea_num)
{
	this -> alpha = alpha;
	this -> beta = beta;
	this -> l1reg = l1reg;
	this -> l2reg = l2reg;
	this -> fea_num = fea_num;
	
	this -> z = new double[fea_num];
	this -> n = new double[fea_num];
	this -> w = new double[fea_num];

	for(uint32_t index = 0; index < fea_num; ++index)
	{
		z[index] = 0;
		n[index] = 0;
		w[index] = 0;
	}
}

Ftrl::Ftrl()
{
	this -> alpha = 0;
	this -> beta = 0;
	this -> l1reg = 0;
	this -> l2reg = 0;
	this -> fea_num = 0;
}
/**
 * label:0/1
 * fea: id+
 */
bool Ftrl::train_instance(instance& ins)
{
	vector<uint32_t>& fea_vec = ins.fea_vec;
	uint32_t ins_len = fea_vec.size();
	//update model
	for(uint32_t index = 0; index < ins_len; ++index)
	{
		uint32_t fea_index = fea_vec[index]; 
		double z_value = z[fea_index];
		double n_value = n[fea_index]; 

		if(fabs( z_value ) <= l1reg)
		{
			w[fea_index] = 0;
		}
		else
		{
			w[fea_index] = (sign(z_value) * l1reg - z_value)
					/ (l2reg + (beta + sqrt(n_value)) / alpha);
		}
	}

	double grad = predict_instance(ins) - ins.label;
	
	for(uint32_t index = 0; index < ins_len; ++index)
	{
		uint32_t fea_index = fea_vec[index];
		double n_value = n[fea_index]; 
		double w_value = w[fea_index]; 
		
		double theta = (sqrt(n_value + grad * grad) - sqrt(n_value)) / alpha;
		z[fea_index] += grad - theta * w_value;
		n[fea_index] += grad * grad;
	}
	return true;
}

bool Ftrl::train_line(string& line)
{
	instance ins;
	if(parse_line(line, ins))
	{
		return train_instance(ins);
	}
	else
	{
		return false;
	}
}

double Ftrl::predict_instance(instance& ins)
{
	return sigmod(inner_product(ins.fea_vec));
}

double Ftrl::predict_instance_static(instance& ins)
{
	vector<uint32_t>& fea_list = ins.fea_vec;

	double inner = 0.0;
	vector<uint32_t>::const_iterator iter = fea_list.begin();
	map<int, double>::iterator map_iter;
	while(iter != fea_list.end())
	{
		map_iter = fea_weight_map.find(*iter);
		if(map_iter != fea_weight_map.end())
		{
			inner += map_iter->second;
		}
		++iter;
	}
	return sigmod(inner);
}

double Ftrl::predict_line(string& line)
{
	instance ins;
	parse_line(line, ins);
	return predict_instance(ins);
}

/**Model format:
 * alpha
 * beta
 * l1reg
 * l2reg
 * fea_num
 * z n w
 */
bool Ftrl::save_model(string& file,bool save_aux)
{
	ofstream model_file(file);
	model_file << alpha << endl;
	model_file << beta << endl;
	model_file << l1reg << endl;
	model_file << l2reg << endl;
	model_file << fea_num << endl;

    if(save_aux){
	    for(uint32_t index = 0; index < fea_num; ++index)
	    {
		    model_file << z[index] << " " << n[index] << " " << w[index] << endl;
	    }
    }else{
        for(uint32_t index = 0; index < fea_num;++index)
        {
            model_file << 0 << " " << 0 << " " << w[index] << endl;
        }
    }

	model_file.close();
	return true;
}

bool Ftrl::load_model(string& file)
{
	ifstream model_file(file);
//	release();
	model_file >> alpha;
	model_file >> beta;
	model_file >> l1reg;
	model_file >> l2reg;
	model_file >> fea_num;

	z = new double[fea_num];
	n = new double[fea_num];
	w = new double[fea_num];

	for(uint32_t index = 0; index < fea_num; ++index)
	{
		model_file >> z[index] >> n[index] >> w[index];
	}
	return true;
}

void Ftrl::load_train_data(ifstream &is)
{
	string line;
	instance ins;

	/*long buffer_size = 512 * 1024 * 1024;
	char* local_buffer = new char[buffer_size];
	is.rdbuf()->pubsetbuf(local_buffer, buffer_size);
*/
	int cnt = 0;
	while(getline(is,line))
	{
		ins.reset();
		parse_line(line,ins);
		train_data.push_back(ins);
        cnt++;
		if(cnt % 1000000 == 0)
			cout << "load instance : " << cnt << endl;
	}
}

void Ftrl::random_shuffle()
{
	srand((int)time(0));
	//init slot
	for(unsigned int i = 0;i < train_data.size();++i)
	{
		slot.push_back(i);
	}
	std::random_shuffle(slot.begin(),slot.end());
}

bool Ftrl::train_batch()
{
	for(unsigned int i = 0;i < slot.size();++i)
	{
		instance ins = train_data[slot[i]];
		train_instance(ins);
		if(i % 1000000 == 0)
			cout << "train instance : " << i << endl;
	}
	return true;
}

bool Ftrl::load_static_model(string& file)
{
	ifstream model_file(file);
	release();
	model_file >> alpha;
	model_file >> beta;
	model_file >> l1reg;
	model_file >> l2reg;
	model_file >> fea_num;

	double w;
	double z = 0;
	double n = 0;

	for(uint32_t index = 0; index < fea_num; ++index)
	{
		model_file >> z >> n >> w;
		if(w != 0)
		{
			fea_weight_map[index] = w;
		}
	}
	return true;
}

void Ftrl::release()
{
	if(w != NULL)
	{
		delete [] w;
		w = NULL;
	}
	if(n != NULL)
	{
		delete [] n;
		n = NULL;
	}
	if(z != NULL)
	{
		delete [] z;
		z = NULL;
	}
}

bool Ftrl::parse_line(string& line)
{
	instance ins;
	return parse_line(line, ins);
}

/**format:
 * label id id +
 */
bool Ftrl::parse_line(string& line, instance& ins)
{
	vector<string> fields;
	util::str_util::split(line, " ", fields);	
	ins.label = atoi(fields[0].c_str());
	for(size_t index = 1; index < fields.size(); ++index)
	{
		ins.fea_vec.push_back((uint32_t)(atoi(fields[index].c_str())));
	}
	return true;
}

double Ftrl::sigmod(double total_weight)
{
	return 1 / (1 + exp(-total_weight));
}

double Ftrl::inner_product(vector<uint32_t>& fea_vec)
{
	double inner = 0.0;
	vector<uint32_t>::const_iterator iter = fea_vec.begin();
	while(iter != fea_vec.end())
	{
		inner += w[*iter];
		++iter;
	}
	return inner;
}
}
