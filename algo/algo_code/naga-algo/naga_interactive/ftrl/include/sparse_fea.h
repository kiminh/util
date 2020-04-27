#ifndef SPARSE_FEA_H
#define SPARSE_FEA_H

#include <vector>

namespace fea
{
typedef uint32_t KeyType;
struct fea_item
{
	KeyType fea_index;
    float fea_val;
};

typedef std::vector<fea_item> fea_list;
struct instance
{
	int label;
	fea_list fea_vec;
	void reset()
	{
		label = 0;
		fea_vec.clear();
	}
};
}
#endif
