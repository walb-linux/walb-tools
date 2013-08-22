.PHONY: all test echo_binaries build clean rebuild install depend

CXX = g++-4.8.1
CC = gcc-4.8.1

OPT_FLAGS =
ifeq ($(DEBUG),1)
OPT_FLAGS += -g -DDEBUG -DWALB_DEBUG
else
OPT_FLAGS += -O2 -DNDEBUG
endif
ifeq ($(PROF),1)
OPT_FLAGS += -pg
else
endif

INCLUDES_GLOBAL = -I./cybozulib/include -I./include -I./src
INCLUDES_WALB = -I./walb/include -I./walb/tool

CFLAGS = -Wall -Wextra -D_FILE_OFFSET_BITS=64 $(OPT_FLAGS) $(INCLUDES_GLOBAL) $(INCLUDES_WALB)
CXXFLAGS = -std=c++11 -pthread $(CFLAGS)

ifeq ($(STATIC),1)
LDFLAGS = -static
LDLIBS = -Wl,--whole-archive -lpthread -Wl,--no-whole-archive
else
LDFLAGS = -Wl,-R,'$$ORIGIN'
LDLIBS = -lpthread
endif

LDLIBS_AIO = -laio
LDLIBS_COMPRESS = -lsnappy -llzma -lz

HEADERS = $(wildcard src/*.hpp src/*.h include/*.hpp include/*.h utest/*.hpp)
BIN_SOURCES = $(wildcard binsrc/*.cpp)
SOURCES = $(wildcard src/*.cpp) $(BIN_SOURCES)
OBJECTS = $(patsubst %.cpp,%.o,$(SOURCES))
BINARIES = $(patsubst %.cpp,%,$(BIN_SOURCES))
TEST_BINARIES = $(patsubst %.cpp,%,$(wildcard utest/*.cpp))

all: build
build: Makefile.depends $(BINARIES)

test: $(TEST_BINARIES)
	@echo not yet implmenented

echo_binaries:
	@echo $(BINARIES)

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $< -o $(patsubst %.cpp,%.o,$<)
.c.o:
	$(CC) $(CFLAGS) -c $< -o $(patsubst %.cpp,%.o,$<)

binsrc/%: binsrc/%.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS) $(LDLIBS_AIO) $(LDLIBS_COMPRESS)

utest/test_compressor: utest/test_compressor.o src/compressor.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS) src/compressor.o $(LDLIBS_COMPRESS)
utest/%: utest/%.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f $(BINARIES) $(TEST_BINARIES) src/*.o binsrc/*.o utest/*.o

rebuild:
	$(MAKE) clean
	$(MAKE) all

install:
	@echo not yet implemented

depend: Makefile.depends

Makefile.depends: $(SOURCES) $(HEADERS)
	$(CXX) -MM $(SOURCES) $(CXXFLAGS) |sed -e 's|^\(.\+\.o:\)|src/\1|' > Makefile.depends

-include Makefile.depends
