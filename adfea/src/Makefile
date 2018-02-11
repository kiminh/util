CXX = g++
INC = -I. -I../include
CFLAGS = -std=c++0x -fPIC -fvisibility=hidden -Wall -O2 -Wno-deprecated
LFLAGS = 
LIBDIR = ../lib
LIBS =  cityhash
BIN = ../bin
all:
	mkdir -p $(LIBDIR)
	mkdir -p $(BIN)
	$(CXX) -o $(BIN)/adfea adfea.cpp beta.cpp conf_util.cpp str_util.cpp combined_fea.cpp direct_fea.cpp device_id_fea.cpp location_fea.cpp ip_fea.cpp appid_fea.cpp creative_fea.cpp domain_fea.cpp anycombined_fea.cpp installed_num_fea.cpp extractor.cpp fea_base.cpp offerclkconv_fea.cpp diffclkconv_fea.cpp offer_price_fea.cpp deviceinfo_fea.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))
	#$(CXX) -o $(BIN)/ffm_adfea ffm_adfea.cpp beta.cpp conf_util.cpp str_util.cpp combined_fea.cpp direct_fea.cpp device_id_fea.cpp location_fea.cpp ip_fea.cpp appid_fea.cpp creative_fea.cpp domain_fea.cpp usershowclk_fea.cpp apptrade_fea.cpp appname_fea.cpp user_interest_fea.cpp appinfo_fea.cpp extractor.cpp fea_base.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))
#	$(CXX) -o $(BIN)/featable featable.cpp beta.cpp conf_util.cpp str_util.cpp combined_fea.cpp direct_fea.cpp device_id_fea.cpp location_fea.cpp ip_fea.cpp appid_fea.cpp creative_fea.cpp domain_fea.cpp anycombined_fea.cpp extractor.cpp fea_base.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))

clean:
	rm -rf *.o *.dSYM $(BIN)/adfea 
