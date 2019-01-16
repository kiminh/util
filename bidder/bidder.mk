LIB_ADFEA_SOURCE := \
        model/fea/beta.cpp \
        model/fea/combined_fea.cpp \
        model/fea/direct_fea.cpp \
        model/fea/extractor.cpp \
        model/fea/fea_base.cpp \
        model/fea/device_id_fea.cpp \
        model/fea/location_fea.cpp \
        model/fea/conf_util.cpp \
        model/fea/ip_fea.cpp  \
        model/fea/domain_fea.cpp \
        model/fea/installed_num_fea.cpp

$(eval $(call library,bayes_adfea, $(LIB_ADFEA_SOURCE), jsoncpp))

LIB_MODEL_SOURCE := \
        model/ModelDict.cpp \
        model/CTRLRModel.cpp \
        model/CTRLRKVModel.cpp \
        model/CVRLRModel.cpp \
        model/CTRModelBase.cpp \
        model/CVRModelBase.cpp \
        model/CTRModelMgr.cpp \
        model/CVRModelMgr.cpp \
        model/CPMBidder.cpp \
        model/CPCLRBidder.cpp \
        model/CPALRBidder.cpp \
        model/CPMBidMgr.cpp \
        model/CPCBidMgr.cpp \
        model/CPABidMgr.cpp \
        model/bidderbase.cpp \
        model/CPCLinearBidder.cpp \
        model/CVRMixModel.cpp \
        model/CVRMixModelMgr.cpp\
        model/AntiModel.cpp \
        model/AntiModelMgr.cpp \
        model/CPCConcaveBidder.cpp \
        model/CTRFMFTRLModel.cpp  \
        model/CPXLRBidder.cpp \
        model/CPXBidMgr.cpp \
        model/CVRGBDTModel.cpp \
        model/CVRFMFTRLModel.cpp \
        model/gbdt.cpp

$(eval $(call library,bayes_model, $(LIB_MODEL_SOURCE)))

# $(eval $(call include_sub_make,model))

LIB_BIDDING_AGENT_SOURCE := \
        BayesBidWorker.cpp \
        bidding_agent.cc \
        bayes_bidding_agent_ex.cc

LIB_BIDDING_AGENT_LINK := \
        ACE \
        arch \
        utils \
        jsoncpp \
        boost_thread \
        zmq \
        opstats \
        bid_request \
        rtb \
        services \
        bayes_adfea \
        bayes_model

$(eval $(call library,bidding_agent, $(LIB_BIDDING_AGENT_SOURCE), $(LIB_BIDDING_AGENT_LINK)))

$(eval $(call program,Bidder, bidding_agent boost_program_options, bayes_bidding_agent_runner.cc))
