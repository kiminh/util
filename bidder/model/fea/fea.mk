LIB_ADFEA_SOURCE := \
        beta.cpp \
        combined_fea.cpp \
        direct_fea.cpp \
        extractor.cpp \
        fea_base.cpp \
        device_id_fea.cpp \
        location_fea.cpp \
        conf_util.cpp \
        ip_fea.cpp  \
        domain_fea.cpp \
        installed_num_fea.cpp

$(eval $(call library,bayes_adfea, $(LIB_ADFEA_SOURCE), jsoncpp))