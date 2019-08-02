/*************************************************************************
    > File Name: extract_main.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 五 12/19 23:01:12 2014
 ************************************************************************/
#include <ctime>
#include <iostream>
#include <fstream>
//#include "conf_util.h"
#include<random>
#include<algorithm>
#include <unordered_set>
#include <unordered_map>
#include<time.h>
#include <sys/time.h>



void print_help()
{
	std::cout <<"Usage:" << "bin input output" << std::endl;
	exit(0);
}


int main(int argc, char* argv[])
{
    using namespace std;

    if(argc < 3)
    {
        cout << "Usage: " << argv[0] << " input output buffer_len(25000)" << endl;
        exit(0);
    }

    ifstream i_f(argv[1]);
    ofstream o_f(argv[2]);

    uint32_t lines_in_memory = 25000;
    if(argc >=4){
        lines_in_memory = strtoul(argv[3], NULL, 0); 
    }

    struct timeval start, end;
    gettimeofday( &start, NULL );

    srand((int)time(0));

    string line;
    uint32_t counter = 0;
    while(getline(i_f, line))
    {
        counter +=1;
    }
    cout << "total cnt: " << counter << endl;

    vector<uint32_t> order;
    for(uint32_t i = 0 ;i < counter ;i++){
        order.push_back(i);   
    }

    random_shuffle(order.begin(), order.end());
    int epoch = 0;
    while(!order.empty()){
        std::unordered_map<uint32_t,string> current_lines;
        uint32_t current_lines_count = 0;
        vector<uint32_t> current_chunk(lines_in_memory);
        if(order.begin() + lines_in_memory < order.end()){
            current_chunk.assign(order.begin(),order.begin()+lines_in_memory);
            //for(auto iter = order.begin(); iter != order.begin() + lines_in_memory; ){
            //    iter = order.erase(iter);
            //}
            order.erase(order.begin(),order.begin()+lines_in_memory);
        }else{
            current_chunk.assign(order.begin(),order.end());
            order.clear();
        }

        std::unordered_set<uint32_t> current_chunk_set;
        uint32_t current_chunk_length = current_chunk.size();
        for(size_t i = 0 ;i < current_chunk_length;i++){
            current_chunk_set.insert(current_chunk[i]);   
        }


        i_f.clear();
        i_f.seekg(0,ios::beg);
        uint32_t count = 0;
        while(getline(i_f,line)){
            //    
            std::unordered_set<uint32_t>::iterator iter = current_chunk_set.find(count);
            if(iter != current_chunk_set.end()){
                //urrent_lines[count] = line
                current_lines[count] = line;
                current_lines_count += 1;
                if(current_lines_count == current_chunk_length){
                    break;    
                }
            }
            count += 1;
            if(count % 100000 == 0){
                cout << "count :" << count << endl;    
            }

        }

        cout << "writing..." << endl;
        cout << "current_chukd: " << current_chunk.size() << " current_line:" <<  current_lines.size() << endl;


        for(size_t i = 0; i < current_chunk.size();i++){
            //cout << "i:  " << i << " current_chunk_i:" << current_chunk[i] << endl;
            o_f << current_lines[current_chunk[i]] << endl;
        }
        uint32_t lines_saved = current_chunk_length + epoch * lines_in_memory;
        epoch +=1;
        cout << "pass " << epoch  << " complete " << lines_saved  << " lines saved" << endl;



    }
    gettimeofday( &end, NULL );
    cout << "cost time:  " <<  end.tv_sec - start.tv_sec  + float(end.tv_usec - start.tv_usec)/1000000 << endl;
    return 0;
}
