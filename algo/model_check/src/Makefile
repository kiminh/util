CXX = g++
INC = -I. -I../include
CFLAGS = -std=c++0x -fPIC -fvisibility=hidden -O2 -Wall -Wno-deprecated
LFLAGS = 
LIBDIR = ../lib
LIBS = 
BIN = ../bin
all:
	mkdir -p $(BIN)
	$(CXX) -o $(BIN)/model_check ModelCheck.cpp conf_util.cpp str_util.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))


clean:
	rm -rf *.o *.dSYM $(BIN)/model_check
