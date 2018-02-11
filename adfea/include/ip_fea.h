//
// Created by starsnet on 15/5/6.
//

#ifndef BAYES_RTBKIT_IP_FEA_H
#define BAYES_RTBKIT_IP_FEA_H

#include "fea_base.h"

namespace fea{
    class ip_fea : public fea_base {
    public:
        virtual bool init();

        virtual bool extract_fea(const record &record, fea_result &result);

    private:
        int m_ip_index;
        int m_ip_bit;
    };
}

#endif //BAYES_RTBKIT_IP_FEA_H
