#ifndef SPARSE_FEA_H
#define SPARSE_FEA_H

#include <vector>

namespace fea
{
struct fea_item
{
	uint32_t fea_index;
};

typedef uint32_t KeyType;
typedef std::vector<KeyType> fea_list;

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
