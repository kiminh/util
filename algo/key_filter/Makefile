CXX = g++

DFLAGS = -lm
#CFLAGS = -O3 -Wall -Iinclude -std=c++0x
CFLAGS = -O3 -Iinclude -std=c++0x

DIR_SRC = ./src

SRC = $(wildcard $(DIR_SRC)/*.cc)

OBJ = $(patsubst %.cc,$(DIR_SRC)/%.o,$(notdir $(SRC)))

target = ./bin/key_filter

$(target) : $(OBJ)
	$(CXX) $(OBJ) -o $@ $(LDFLAGS)

$(DIR_SRC)/%.o : $(DIR_SRC)/%.cc
	$(CXX) $(CFLAGS) -c $< -o $@

.PHONY:clean

clean:
	$(RM) $(DIR_SRC)/*.o
	$(RM) ./bin/key_filter
