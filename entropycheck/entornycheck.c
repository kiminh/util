#include <iostream>
#include <cstdio>
#include <fstream>
#include <vector>
#include <string>
#include <stdint.h>
#include <stdlib.h>
#include <cmath>
#include <map>

using namespace std;

struct vect
{
	int neg;
	int pos;
	vect(int n,int p){
		neg = n;
		pos = p;
	}
};

void split(const std::string& line, const std::string delim, 
		std::vector<std::string>& elems)
{
	size_t pos = 0;
	size_t len = line.length();
	size_t delim_len = delim.length();
	
	if (delim_len == 0) 
	{
		return;
	}
	//没有查找完	
	while (pos < len)
	{
		size_t find_pos = line.find(delim, pos);
		
		if (find_pos == string::npos)
		{
			elems.push_back(line.substr(pos));
			break;
		}
		if(find_pos != pos)
		{
			elems.push_back(line.substr(pos, find_pos - pos));
		}
		pos = find_pos + delim_len;
	}
}

int main(int argc,char **argv)
{
	if(argc < 3)
	{
		cout << "Usage:entorny train_ins fea_index" << endl;
		return -1;
	}
	uint32_t fea_index = atoi(argv[2]);
	
	//cout the label
	int label_value[2] = {0};
	map<uint32_t,vect> fea_index_map;
	map<uint32_t,vect>::iterator iter;
	
	ifstream train_stream(argv[1]);
	if(train_stream.fail())
	{
		cout << "open train file error!" << endl;
		return -1;
	}
	
	string line;
	vector<string> fields;

    int total_ins = 0;
	while(getline(train_stream,line))
	{
        fields.clear();
		split(line," ",fields);
        int label = atoi(fields[0].c_str());

		if(label == 1)
		{
			label_value[1]++;
			uint32_t key = (uint32_t)atoi(fields[fea_index + 1].c_str());
			iter = fea_index_map.find(key);
			if(iter != fea_index_map.end()) //存在key
			{
				iter->second.pos++;
			}else{
				fea_index_map.insert(make_pair(key,vect(0,1))); 
			}
		}else{
			label_value[0]++;
			uint32_t key = (uint32_t)atoi(fields[fea_index + 1].c_str());
			iter = fea_index_map.find(key);
			if(iter != fea_index_map.end()) //存在key
			{
				iter->second.neg++;
			}else{
				fea_index_map.insert(make_pair(key,vect(1,0)));
			}
		}
        total_ins++;
        if(total_ins % 1000000 == 0)
            cout << "total_ins: " << total_ins << endl; 
	}
	
	int sum = label_value[0] + label_value[1];
	double p_0 = double(label_value[0]) / sum;
	double p_1 = double(label_value[1]) / sum;

    cout << label_value[0] <<" "<< label_value[1] << " " << sum << endl;

	double HD = - p_0 * log(p_0) - p_1 * log(p_1);
	//cout << "HD : " << HD << endl;	
	double HD_A = 0.0;

    double D = 0.0;
	for(iter = fea_index_map.begin();iter != fea_index_map.end();iter++)
	{
		int neg = iter->second.neg;
		int pos = iter->second.pos;
		if( neg == 0 ||  pos == 0 ){
			D += 0.0;
		}else{
            int sum_d = neg + pos;
            double p_d_0 = double(neg) / sum_d;
            double p_d_1 = double(pos) / sum_d;

			D += ((double)sum_d / sum) * ( - (double)neg / sum_d * log(p_d_0) - (double)pos / (sum_d) * log(p_d_1) );
		}
	}
    //cout << "D : " << D << endl;
	HD_A = HD - D;
	cout << HD_A << endl;
	return 0;
}
