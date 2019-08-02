CXX = g++
INC = -I. -I../include
CFLAGS = -std=c++0x -fPIC -fvisibility=hidden -O2 -Wall -Wno-deprecated
LFLAGS = 
LIBDIR = ../lib
LIBS = 
BIN = ../bin
all:
	$(CXX) -o $(BIN)/gbdt_predict predict.cpp conf_util.cpp gbdt.cpp  str_util.cpp jsoncpp.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))
	$(CXX) -o $(BIN)/fea_index_map fea_index_map.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))
clean:
	rm -rf *.o *.dSYM $(BIN)/gbdt_predict
