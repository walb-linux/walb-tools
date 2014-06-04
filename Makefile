.PHONY: all test test_all echo_binaries build clean rebuild install stest

CXX = clang++
CC = clang

OPT_FLAGS =
ifeq ($(DEBUG),1)
OPT_FLAGS += -g -DDEBUG -DWALB_DEBUG -DCYBOZU_EXCEPTION_WITH_STACKTRACE -DCYBOZU_STACKTRACE_WITH_BFD_GPL
OPT_FLAGS += -DPACKAGE  -DPACKAGE_VERSION  # required by bfd.h.
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
LDFLAGS = -static -static-libgcc -static-libstdc++ -L./src
LDLIBS = -Wl,--whole-archive -lpthread -lrt -Wl,--no-whole-archive
else
LDFLAGS = -Wl,-R,'$$ORIGIN' -L./src
LDLIBS = -lpthread -lrt
endif
ifeq ($(DEBUG),1)
LDFLAGS += -rdynamic
LDLIBS += -lbfd
endif

LDLIBS_LOCAL = -lwalb-tools
LDLIBS_AIO = -laio
LDLIBS_COMPRESS = -lsnappy -llzma -lz
LDLIBS += $(LDLIBS_LOCAL) $(LDLIBS_AIO) $(LDLIBS_COMPRESS)

HEADERS = $(wildcard src/*.hpp src/*.h include/*.hpp include/*.h utest/*.hpp)
BIN_SOURCES = $(wildcard binsrc/*.cpp)
OTHER_SOURCES = $(wildcard src/*.cpp)
TEST_SOURCES = $(wildcard utest/*.cpp)
SOURCES = $(OTHER_SOURCES) $(BIN_SOURCES) $(TEST_SOURCES)
DEPENDS = $(patsubst %.cpp,%.depend,$(SOURCES))
BINARIES = $(patsubst %.cpp,%,$(BIN_SOURCES))
TEST_BINARIES = $(patsubst %.cpp,%,$(TEST_SOURCES))
LOCAL_LIB = src/libwalb-tools.a
LOCAL_LIB_OBJ = $(patsubst %.cpp,%.o,$(OTHER_SOURCES))

all: build
build: $(BINARIES)

utest: $(TEST_BINARIES)
utest_all: $(TEST_BINARIES)
	@for t in $(TEST_BINARIES); do \
	    ./$$t; \
	done 2>&1 |tee test.log |grep ^ctest:name
	grep ctest:name test.log | grep -v "ng=0, exception=0" || echo "all unit tests succeed"
itest: $(BINARIES)
	$(MAKE) -C itest/wdiff
	$(MAKE) -C itest/wlog

echo_binaries:
	@echo $(BINARIES)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

$(LOCAL_LIB): $(LOCAL_LIB_OBJ)
	ar rv $(LOCAL_LIB) $(LOCAL_LIB_OBJ)

binsrc/%: binsrc/%.o $(LOCAL_LIB)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

utest/%: utest/%.o $(LOCAL_LIB)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

clean: cleanobj cleandep
	rm -f $(BINARIES) $(TEST_BINARIES) $(LOCAL_LIB)
cleanobj:
	rm -f src/*.o binsrc/*.o utest/*.o
cleandep:
	rm -f src/*.depend binsrc/*.depend utest/*.depend

rebuild:
	$(MAKE) clean
	$(MAKE) all

install:
	@echo not yet implemented

binsrc/%.depend: binsrc/%.cpp
	$(CXX) -MM $< $(CXXFLAGS) |sed -e 's|^\(.\+\.o:\)|binsrc/\1|' > $@
src/%.depend: src/%.cpp
	$(CXX) -MM $< $(CXXFLAGS) |sed -e 's|^\(.\+\.o:\)|src/\1|' > $@
utest/%.depend: utest/%.cpp
	$(CXX) -MM $< $(CXXFLAGS) |sed -e 's|^\(.\+\.o:\)|utest/\1|' > $@

stest: build
	pylint -E --rcfile=/dev/null -f colorized stest/walb_cmd.py stest/scenario0.py
	python stest/scenario0.py

ifneq "$(MAKECMDGOALS)" "clean"
-include $(DEPENDS)
endif
