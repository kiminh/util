$(eval $(call include_sub_make,fea))

LIB_MODEL_SOURCE := \
        ModelDict.cpp \
        CTRLRModel.cpp \
        CTRLRKVModel.cpp \
        CVRLRModel.cpp \
        CTRModelBase.cpp \
        CVRModelBase.cpp \
        CTRModelMgr.cpp \
        CVRModelMgr.cpp \
        CPMBidder.cpp \
        CPCLRBidder.cpp \
        CPALRBidder.cpp \
        CPMBidMgr.cpp \
        CPCBidMgr.cpp \
        CPABidMgr.cpp \
        bidderbase.cpp \
        CPCLinearBidder.cpp \
        CVRMixModel.cpp \
        CVRMixModelMgr.cpp\
        AntiModel.cpp \
        AntiModelMgr.cpp \
        CPCConcaveBidder.cpp \
        CTRFMFTRLModel.cpp  \
        CPXLRBidder.cpp \
        CPXBidMgr.cpp \
        CVRGBDTModel.cpp \
        gbdt.cpp

$(eval $(call library,bayes_model, $(LIB_MODEL_SOURCE)))
