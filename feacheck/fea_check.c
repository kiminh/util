/**
 *   根据l1正则化的结果检测特征有效
 *
 */

#include <iostream>
#include <cstdio>
#include <stdint.h>
#include <cstdlib>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>

#include "str_util.h"

using namespace std;

int ins_col;

int fea_check(vector<uint32_t > train_ins,size_t size,uint32_t fea_index)
{
    vector<uint32_t>::iterator f,b,e;
    b = train_ins.begin();
    e = train_ins.end();

    f = find(b,e,fea_index);
    if(f == e)
        return -1;
    else
        return (f - b) % ins_col;
}

int main(int argc,char **argv)
{
    if(argc < 4)
    {
        cout << "Usage:fea_check shitu_ins fea_index_file fea_name_file" << endl;
        return -1;
    }
	
	ifstream train_stream(argv[1]);
	ifstream fea_index_stream(argv[2]);
	ifstream fea_name_stream(argv[3]);
	
	if(train_stream.fail())
	{
		fprintf(stderr,"open the train instance file fail!");
		return -1;
	}
	
	if(fea_index_stream.fail())
	{
		fprintf(stderr,"open the fea index fail!");
		return -1;
	}
	
	if(fea_name_stream.fail())
	{
		fprintf(stderr,"open the fea index fail!");
		return -1;
	}
	
	//load shitu instance
	vector<uint32_t > train_ins;
	string line;
	int total_ins = 0;
	vector<string> fields;
	vector<uint32_t> fea_vect;
	
	cout << "load train ins..." << endl;
	
	while(getline(train_stream,line))
	{
		fields.clear();
        util::str_util::split(line, " ", fields);
        fea_vect.clear();
 
        if(total_ins == 1)
            ins_col = fields.size() - 1;

		for(size_t index = 1; index < fields.size(); ++index)
		{
			train_ins.push_back((uint32_t)(atoi(fields[index].c_str())));
		}
		total_ins++;
		if(total_ins % 1000000 == 0)
			std::cout << "load instance :" << total_ins << std::endl;
	}

    cout << "load train instance complete..." << endl;

	vector<uint32_t> fea_index;
	while(getline(fea_index_stream,line))
	{
		fea_index.push_back(atoi(line.c_str()));
	}
	
	map<string,int> fea_name_map;
	vector<string> fea_name_vect;
	
	while(getline(fea_name_stream,line))
	{
		fea_name_map[line] = 0;
		fea_name_vect.push_back(line);
	}
	
    cout << "begin to cal..." << endl;

	for(size_t index;index < fea_index.size();index++)
	{
        //if(index % 10000 == 0)
            cout << index << endl;
		int loc = fea_check(train_ins,fea_name_vect.size(),fea_index[index]);
		if(loc != -1)
			fea_name_map[fea_name_vect[loc]]++;
	}
	
	ofstream result("result.dat");
    map<string,int>::iterator it;
    for(it = fea_name_map.begin();it!=fea_name_map.end();++it)
    {
        result << it->first << " : " << it->second << endl;
    }
	
	result.close();
	train_stream.close();
	fea_index_stream.close();
	fea_name_stream.close();
	
    return 0;
}
