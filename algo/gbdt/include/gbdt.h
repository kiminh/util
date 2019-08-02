#ifndef _TREE_MODEL
#define _TREE_MODEL

#include <string>
#include <vector>
#include "json/json.h"
#include "sparse_fea.h"

namespace alg{
	
typedef struct node{
    int nodeid;
    int depth;
    int yes;
    int no;
    int missing;
    uint32_t split;
    double split_condition;
    struct node *rchild;
    struct node *lchild;
    bool is_leaf;
    double leaf;
}tree;

class gbdt {
public:
    gbdt();
    ~gbdt();
    
    void release(tree *root);

    bool load_tree_model(std::string model_file);
    void print_tree();
    void print_one_tree(tree *root);
    void parse_tree(const Json::Value &root);
    void parse_one_tree(const Json::Value &value,int index,tree * &t,std::vector<int> &);

    double one_tree_predict_line(fea::instance &ins,size_t index);
    double predict_line(fea::instance &ins);
    void parse_line(std::string& line, fea::instance& ins);
    double sigmoid(double inx);
    void print_leafs();
    size_t get_leaf_index(fea::instance &ins,size_t index);
    void get_trees_leaf_index(fea::instance &ins,std::vector<size_t> &);
    size_t get_number_of_leafs();

private:
    tree **trees;
    size_t tree_num;
    std::vector<std::vector<int> > leaf_set;
};

}

#endif
