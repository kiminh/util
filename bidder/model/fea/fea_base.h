/*************************************************************************
 > File Name: fea.h
 > Author: starsnet83
 > Mail: starsnet83@gmail.com
 > Created Time: 一  1/ 5 11:16:17 2015
 ************************************************************************/

#ifndef __FEA_H__
#define __FEA_H__

#include <string>
#include <vector>
#include <functional>
#include <map>
#include "fea_record.h"
#include "fea_result.h"
#include "bidmax/common/str_util.h"

namespace fea
{
struct fea_arg
{
    std::string fea_name;
    std::string method;
    std::string slot;
    std::string arg;
    std::string dep;
    bool is_output;

    bool parse_fea_arg(std::string &line)
    {
        std::vector <std::string> items;
        util::str_util::split(line, ";", items);
        for (size_t index = 0; index < items.size(); ++index) {
            util::str_util::trim(items[index]);
        }
        if (items.size() < 3) {
            cout << "Fea conf is error, must large 3 :" << line << endl;
            return false;
        }

        std::vector <std::string> fields;
        util::str_util::split(items[0], "=", fields);

        if (fields[0] == ".fea_name") {
            is_output = false;
            fea_name = fields[1];
        } else if (fields[0] == "fea_name") {
            is_output = true;
            fea_name = fields[1];
        } else {
            return false;
        }

        fields.clear();
        util::str_util::split(items[1], "=", fields);
        if (fields[0] == "method") {
            method = fields[1];
        } else {
            return false;
        }

        fields.clear();
        util::str_util::split(items[2], "=", fields);
        if (fields[0] == "slot") {
            slot = fields[1];
        } else {
            return false;
        }

        for (size_t index = 3; index < items.size(); ++index) {
            fields.clear();
            util::str_util::split(items[index], "=", fields);
            if (fields[0] == "dep") {
                dep = fields[1];
            }
            if (fields[0] == "arg") {
                arg = fields[1];
            }
        }
        return true;
    }

    void clear()
    {
        fea_name.clear();
        method.clear();
        slot.clear();
        arg.clear();
        dep.clear();
        is_output = true;
    }
};

class fea_base
{
public:
    virtual void set_fea_arg(fea_arg &m_fea_arg)
    {
        this->m_fea_arg = m_fea_arg;
    }

    virtual bool check_arg(record &record);

    virtual bool init()
    {
        return true;
    }

    virtual bool extract_fea(const record &record, fea_result &result)
    {
        return true;
    }

    inline std::string get_name()
    {
        return m_fea_arg.fea_name;
    }

    inline bool is_output_fea()
    {
        return m_fea_arg.is_output;
    }

protected:
    virtual void commit_single_fea(const std::string &value,
                                   fea_result &result);

    virtual void commit_combine_fea(uint32_t fdate1, uint32_t fdata2,
                                    fea_result &result);

    virtual uint32_t Hash32(uint32_t fdate1, uint32_t fdata2)
    {
        return (31 + fdate1) * 31 + fdata2;
    }

public:
    fea_base()
    {
        is_output_text = true;
    }

    virtual ~fea_base()
    {
        // do nothing
    }

protected:
    std::hash <std::string> hash_fn;
    bool is_output_text;
    std::vector<int> m_vec_param;
    int m_param_num;
    fea_arg m_fea_arg;

};
}
#endif
