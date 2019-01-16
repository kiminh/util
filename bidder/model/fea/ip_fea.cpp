//
// Created by starsnet on 15/5/6.
//

#include "ip_fea.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

//fea_name=IP**;method=ip_fea;slot=**;dep=ipAddress;arg=**
namespace fea
{
bool ip_fea::init()
{
    if (m_vec_param.size() != 1) {
        return false;
    } else {
        m_ip_index = m_vec_param[0];
        m_ip_bit = 32 - atoi(m_fea_arg.arg.c_str());
        if (m_ip_bit > 32 || m_ip_bit <= 0) {
            return false;
        }
    }
    return true;
}

bool ip_fea::extract_fea(const record &record, fea_result &result)
{
    struct in_addr ip_res;
    inet_pton(AF_INET, record.valueAt(m_ip_index).c_str(), (void *) &ip_res);

    commit_single_fea(to_string(ip_res.s_addr >> m_ip_bit), result);
    return true;
}
}
