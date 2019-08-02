#ifndef _KEY_FILTER_
#define _KEY_FILTER_
#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <cstring>
#include <sstream>
#include <fstream>

#include "dablooms.h"

namespace algo {

typedef std::string Key;

class KeyFilter
{
  public:
    KeyFilter()
      :bloom_file("NULL")
      ,output_file("NULL")
      ,input_file("NULL")
    {
      capacity = 100000000;
      error_rate = 0.05;
      less_count = 1;

      is_first_start = 0; //must check the bloom file
      bloom == NULL;
    }

    virtual ~KeyFilter()
    {
      std::cout << "~KeyFilter" << std::endl;
      if(bloom != NULL){
        for(int i = 0; i < less_count;++i)
        {
          if(bloom[i] != NULL){
            free_counting_bloom(bloom[i]);
            bloom[i] == NULL;
          }
        }
        free(bloom);
        bloom = NULL;
      }
    }

    inline void SetParam(const char *name, const char *val)
    {
      if (!strcmp(name, "capacity"))
        capacity = static_cast<int>(atoi(val));
      if (!strcmp(name, "error_rate"))
        error_rate = static_cast<float>(atof(val));
      if (!strcmp(name, "less_count"))
        less_count = static_cast<float>(atoi(val));
      if (!strcmp(name, "bloom_file"))
        bloom_file = val;
      if (!strcmp(name, "output_file"))
        output_file = val;
      if (!strcmp(name, "input_file"))
        input_file = val;
      if (!strcmp(name, "is_first_start"))
        is_first_start = static_cast<int>(atoi(val));
    }
    
    void Init()
    {
      assert(bloom_file != "NULL" && output_file != "NULL" && input_file != "NULL");
      assert(less_count >= 1);
      
      std::cout << "less_cout=" << less_count << ", output_file=" << output_file 
                << ", input_file=" << input_file << ", bloom_file=" << bloom_file 
                << ", capacity=" << capacity << ", error_rate=" << error_rate
                << ", is_first_start=" << is_first_start << std::endl;
      bloom = (counting_bloom_t **)malloc(sizeof(counting_bloom_t *));
      for(int i = 0;i < less_count;++i)
      {
        std::string _bf = bloom_file + std::to_string(i);
        if(is_first_start)
          bloom[i] = new_counting_bloom(capacity, error_rate, _bf.c_str());
        else //load from file
          bloom[i] = new_counting_bloom_from_file(capacity, error_rate, _bf.c_str());
      }
    }

    void AddKey(Key key)
    {
      for(int i = 0;i < less_count;++i)
      {
        if(!counting_bloom_check(bloom[i], key.c_str(), strlen(key.c_str()))){
          counting_bloom_add(bloom[i], key.c_str(), strlen(key.c_str()));
          return;
        }
      }
    }

    void DelKey(Key key)
    {
      //todo
    }

    bool CheckKey(Key key)
    {
      for(int i = 0;i < less_count;++i)
      {   
        if(counting_bloom_check(bloom[i], key.c_str(), strlen(key.c_str()))){
          return true;
        }   
      }
      return false;
    }

    int getKeyCount(Key key)
    {
      int count = 0;
      for(int i = 0;i < less_count;++i)
      {   
        if(counting_bloom_check(bloom[i], key.c_str(), strlen(key.c_str()))){
          count++;
        }   
      }
      return count;
    }

    bool IsOutPutKey(Key key)
    {
      for(int i = 0;i < less_count;i++)
      {
        if(!counting_bloom_check(bloom[i], key.c_str(), strlen(key.c_str()))) {
          counting_bloom_add(bloom[i], key.c_str(), strlen(key.c_str()));
          return false;
        }
      }
      return true;
    }

    void Split(std::string line, std::vector<std::string> &fea_vec)
    {
      std::stringstream ss;
      ss << line;
      while(ss >> line)
      {
        fea_vec.push_back(line);
      }
    }
    
    virtual void Filter()
    {
      std::ifstream is(input_file.c_str());
      std::ofstream os(output_file.c_str());
      
      assert(is.fail() == false);
      assert(os.fail() == false);

      std::string line;
      std::vector<std::string> fea_vec;
      int cnt = 0;
      int uniq_keys = 0;
      while(getline(is, line))
      {
        fea_vec.clear();
        Split(line, fea_vec);
        os << fea_vec[0] << " ";
        for(int i = 1; i < fea_vec.size(); ++i)
        {   
          /*if(getKeyCount(fea_vec[i]) == less_count)
          {   
            os << fea_vec[i] << " ";
          }else{
            AddKey(fea_vec[i]);
            uniq_keys++;
          }*/
          if(IsOutPutKey(fea_vec[i])){
            os << fea_vec[i] << " ";
          }else{
            uniq_keys++;
          }

          if(i == fea_vec.size() - 1)
            os << std::endl;
        }   
        ++cnt;
        if(cnt % 100000 == 0)
            std::cout << "Process " << cnt << " instance, unique keys " << uniq_keys << std::endl;
      }

      is.close();
      os.close();
    }
  private:
    int capacity;
    float error_rate;
    std::string bloom_file;
    std::string output_file;
    std::string input_file;
    int less_count;
    int is_first_start;

    //counting bloom filter
    counting_bloom_t **bloom;
};
}

#endif
