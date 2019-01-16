/*************************************************************************
    > File Name: sparse_fea.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 四 12/25 00:09:45 2014
 ************************************************************************/

#ifndef SPARSE_FEA_H
#define SPARSE_FEA_H

#include <vector>
#include <algorithm>
namespace fea
{
struct fea_item
{
    uint32_t fea_index;
};

typedef std::vector <uint32_t> fea_list;

struct instance
{
    int label;
    fea_list fea_vec;

    void reset()
    {
        label = 0;
        fea_vec.clear();
    }
    void add(struct instance ins2)
    {
        for (size_t i = 0; i < ins2.fea_vec.size(); i++) {
            fea_vec.push_back(ins2.fea_vec[i]);
        }
    }

    void add_fea(std::vector <uint32_t> fea_vec)
    {
        for (size_t i = 0; i < fea_vec.size(); i++) {
            fea_vec.push_back(fea_vec[i]);
        }
    }

    bool is_missing(uint32_t index)
    {
        std::vector<uint32_t>::iterator result = find(fea_vec.begin(), fea_vec.end(), index);
        if (result == fea_vec.end())
            return true;
        else
            return false;
    }

    int fvalue(uint32_t index)
    {
        if (!is_missing(index)) return 1;
        else return 0;
    }

};
}
#endif
