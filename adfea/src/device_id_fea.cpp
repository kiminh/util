/*************************************************************************
    > File Name: device_id_fea.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: Mon 20 Apr 2015 03:58:53 PM CST
 ************************************************************************/
//fea_name=deviceID;method=device_id_fea;slot=7;dep=didsha1,didmd5
#include "device_id_fea.h"
#include "str_util.h"


namespace fea {
    using namespace std;
    using namespace util;

    bool device_id_fea::init() {
        cout << m_vec_param.size() << endl;
        if (m_vec_param.size() != 2) {
            return false;
        }
        else {
            sha1_index = m_vec_param[0];
            md5_index = m_vec_param[1];
        }
        return true;
    }

    bool device_id_fea::extract_fea(const record &record, fea_result &result) {
        if (!record.valueAt(sha1_index).empty()) {
            commit_single_fea(record.valueAt(sha1_index), result);
        }
        else if (!record.valueAt(md5_index).empty()) {
            commit_single_fea(record.valueAt(md5_index), result);
        }
        else {
            commit_single_fea("",result);
            //is_extract = false;
            //return false;
        }
        is_extract = true;
        return true;
    }
}
