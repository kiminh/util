#include <iostream>
#include <fstream>
#include <cstdlib>
#include <fstream>
#include <string>
#include <cmath>

#include "json/json.h"
#include "gbdt.h"
#include "str_util.h"

namespace alg
{
	
gbdt::gbdt(){
    trees = NULL;
    leaf_set.clear();
}

gbdt::~gbdt()
{
    if(trees != NULL){
        for(size_t index = 0;index < tree_num;++index)
        {
            release(trees[index]);
        }
        free(trees);
        trees = NULL;
    }
}
	
void gbdt::release(tree *root)
{
    if(root){
        release(root->lchild);
        release(root->rchild);
        free(root);
        root = NULL;
    }
}

bool gbdt::load_tree_model(std::string model_file)
{
    std::ifstream ifs(model_file.c_str());
    if(ifs.fail()){
        std::cerr << "tree model file open fail." << std::endl;
        return false;
    }
    
    Json::Reader reader;
    Json::Value root;
    
    if(!reader.parse(ifs,root,false)){
        std::cerr << "parse model json file error." << std::endl;
        return false;
    }
    
    size_t size = root.size();
    this->tree_num = size;
    
    //init leaf set
    for(size_t i = 0;i < size;++i)
    {
        std::vector<int> leaf;
        leaf_set.push_back(leaf);
    }
    
    std::cout << "begin to parse ["<< tree_num  << "] regression tree from json file." << std::endl;
    
    trees = (tree **)malloc(sizeof(struct node*) * size);
    if(trees == NULL)
    {
        std::cerr << "malloc tree error." << std::endl;
        return false;
    }
    
    parse_tree(root);
    return true;
}

void gbdt::parse_tree(const Json::Value &root)
{
    if(root.isNull())
        return;

    for(size_t i = 0;i < this->tree_num;i++){
        parse_one_tree(root,i,trees[i],leaf_set[i]);
    }
}

void gbdt::parse_one_tree(const Json::Value &root,
                            int index,tree * &t,std::vector<int> &leafs)
{
    if(root.isNull()){
        return ;
    }
    
    t = (tree *)malloc(sizeof(struct node));
    t->nodeid = root[index]["nodeid"].asInt();

    if(!root[index]["leaf"].isNull()){
        t->rchild = NULL;
        t->lchild = NULL;
        t->leaf = root[index]["leaf"].asDouble();
		t->is_leaf = true;
        leafs.push_back(t->nodeid);
        return;
    }

    if(!root[index]["depth"].isNull()){
        t->depth = root[index]["depth"].asInt();
    }

    if(!root[index]["split"].isNull()){
        t->split = root[index]["split"].asUInt();
    }

    if(!root[index]["yes"].isNull()){
        t->yes = root[index]["yes"].asInt();
    }

    if(!root[index]["no"].isNull()){
        t->no = root[index]["no"].asInt();
    }

    if(!root[index]["missing"].isNull()){
        t->missing = root[index]["missing"].asInt();
    }

    if(!root[index]["split_condition"].isNull()){
        t->split_condition = root[index]["split_condition"].asDouble();
    }

	t->is_leaf = false;
    Json::Value children = root[index]["children"];
    parse_one_tree(children,0,t->lchild,leafs);
    parse_one_tree(children,1,t->rchild,leafs);
}

//for test
void gbdt::print_one_tree(tree *root)
{
    if(root){
        if(root->is_leaf)
        {
            std::cout << "nodeid: " <<  root->nodeid << " " 
                        << "leaf: " << root->leaf << std::endl;

        }else{
        	std::cout << "nodeid: " <<  root->nodeid << " " 
		          << "split: " << root->split << " " 
                  << "yes: " << root->yes << " " 
                  << "no: " << root->no << " "
                  << "depth: " << root->depth << " "
				  << "split_cond: " << root->split_condition << std::endl;		  

        }
        print_one_tree(root->lchild);
        print_one_tree(root->rchild);
	}
}

void gbdt::print_tree()
{
	for(size_t index = 0;index < this->tree_num;index++){
		std::cout << "trees[" << index << "]:" << std::endl; 
		print_one_tree(trees[index]);
	}
}

size_t gbdt::get_leaf_index(fea::instance &ins,size_t index)
{ 
    size_t pos = 1;
    tree *root = trees[index];
    std::vector<int> &leafs = leaf_set[index];
    
    while(!root->is_leaf)
    {
        if(ins.is_missing(root->split)){
            root = root->lchild;
        }else{
            if(ins.fvalue(root->split) < root->split_condition){
                root = root->lchild;
            }else{
                root = root->rchild;
            }
        }
    }
    
    std::vector<int>::iterator iter = leafs.begin();
    while(iter != leafs.end()){
        if(*iter == root->nodeid){
            break;
        }
        pos++;
        iter++;
    }
	return pos;
}

void gbdt::get_trees_leaf_index(fea::instance &ins,std::vector<size_t> &index)
{
    size_t tree_leaf = 0;
    for(size_t i = 0;i < this->tree_num;++i)
    {
        size_t pos = get_leaf_index(ins,i);
        index.push_back(pos + tree_leaf);
        tree_leaf += leaf_set[i].size();
    }
}

double gbdt::one_tree_predict_line(fea::instance &ins,size_t index)
{
    tree *root = trees[index];
    
    while(!root->is_leaf)
    {
        if(ins.is_missing(root->split)){
            root = root->lchild;
        }else{
            if(ins.fvalue(root->split) < root->split_condition){
                root = root->lchild;
            }else{
                root = root->rchild;
            }
        }
    }
    return root->leaf;
}

double gbdt::predict_line(fea::instance &ins)
{
	double psum = 0.0;
	for(size_t index = 0;index < this->tree_num;index++)
	{
		psum += one_tree_predict_line(ins,index);
	}
	return sigmoid(psum);
}

void gbdt::parse_line(std::string& line, fea::instance& ins)
{
	std::vector<std::string> fields;
	util::str_util::split(line, " ", fields);	
	ins.label = atoi(fields[0].c_str());
	for(size_t index = 1; index < fields.size(); ++index)
	{
		ins.fea_vec.push_back((uint32_t)(atoi(fields[index].c_str())));
	}
}

double gbdt::sigmoid(double inx)
{
    return 1.0f / (1.0f + std::exp(-inx));
}

void gbdt::print_leafs()
{
    std::cout << "the leafs of tree is: " << std::endl;
    for(size_t index = 0;index < this->tree_num;++index)
    {
        std::cout << "trees["  << index << "]:" << " ";
        for(size_t j = 0;j < leaf_set[index].size();j++)
        {
           std::cout << leaf_set[index][j] << " " ;
        }
        std::cout << std::endl;
    }
}
size_t gbdt::get_number_of_leafs()
{
    size_t sum_leafs = 0;
    
    std::vector<std::vector<int> >::iterator iter = leaf_set.begin();
    while(iter != leaf_set.end()){
        sum_leafs += (*iter).size();
        iter++;
    }
    return sum_leafs;
}

}
