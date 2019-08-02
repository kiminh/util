#include <iostream>
#include <fstream>
#include <string>

#include "gbdt.h"
#include "conf_util.h"

using namespace alg;

int main(int argc,char **argv)
{
	if(argc < 2){
		std::cout <<"Usage:" 
			<< "predict conf_file" << std::endl;
	    return -1;
	}
	
    std::string conf_file(argv[1]);
    util::conf_util gbdt_conf;
    gbdt_conf.parse(conf_file);
    
    std::string model_file = gbdt_conf.getItem<std::string>("model_file");
    std::string input_instance_file = gbdt_conf.getItem<std::string>("input_instance_file");
    std::string output_instance_file = gbdt_conf.getItem<std::string>("output_instance_file");
    uint64_t fea_num = gbdt_conf.getItem<uint64_t>("fea_num");
    
    gbdt m_gbdt;
    m_gbdt.load_tree_model(model_file);
    
    bool debug = false;
    if(gbdt_conf.has_item("debug")){
        debug = gbdt_conf.getItem<bool>("debug");
    }
    
    if(debug){
        m_gbdt.print_tree();
        m_gbdt.print_leafs();
        std::cout << "the number of the leafs: " << m_gbdt.get_number_of_leafs() << std::endl;
    }
    
    std::ifstream predict_stream(input_instance_file.c_str());
    std::ofstream out_file(output_instance_file.c_str());
    
    int total_num = 0;
    std::cout << "Begin to predict and output GBDT leaf....." << std::endl;
    fea::instance ins;
	std::string line;
    std::vector<size_t> index;
    
    while(getline(predict_stream, line))
    {
        index.clear();
        ++total_num;
        if(total_num % 100000 == 0)
        {
            std::cout << "predict lines: " <<  total_num << std::endl;
        }
        
        ins.reset();
        m_gbdt.parse_line(line, ins);
        m_gbdt.get_trees_leaf_index(ins,index);
        
        out_file << ins.label << " " ;
        //output LR feature
        for(size_t i = 0;i < ins.fea_vec.size();i++)
        {
            out_file << ins.fea_vec[i] << " ";
        }
        //output gbdt leaf
        for(size_t i = 0;i < index.size();i++)
            out_file << index[i] + fea_num << " ";
        out_file << std::endl;
    }
    
    predict_stream.close();
    out_file.close();
    
    return 0;
}
