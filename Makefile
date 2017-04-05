.PHONY: all utest utest_all itest echo_binaries build clean rebuild install stest pylint manpages core version_cpp

CXX = clang++
CC = clang

OPT_FLAGS =-DCYBOZU_EXCEPTION_WITH_STACKTRACE -g
ifeq ($(DEBUG),1)
OPT_FLAGS += -DDEBUG -DWALB_DEBUG
ifeq ($(BFD),1)
OPT_FLAGS += -DCYBOZU_STACKTRACE_WITH_BFD_GPL
endif
else
OPT_FLAGS += -O2 -ftree-vectorize -DNDEBUG
endif
ifeq ($(PROF),1)
OPT_FLAGS += -pg
else
endif
ifeq ($(ENABLE_EXEC_PROTOCOL),1)
OPT_FLAGS += -DENABLE_EXEC_PROTOCOL
endif
ifeq ($(DISABLE_COMMIT_ID),1)
OPT_FLAGS += -DDISABLE_COMMIT_ID
endif

INCLUDES_GLOBAL = -I./cybozulib/include -I./include -I./src
INCLUDES_WALB = -I./walb/include

CFLAGS = -Wall -Wextra -D_FILE_OFFSET_BITS=64 $(OPT_FLAGS) $(INCLUDES_GLOBAL) $(INCLUDES_WALB) -DCYBOZU_SOCKET_USE_EPOLL
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
ifeq ($(BFD),1)
LDLIBS_BFD = -lbfd
else
LDLIBS_BFD =
endif
endif

LDLIBS_LOCAL = -lwalb-tools
LDLIBS_AIO = -laio
LDLIBS_COMPRESS = -lsnappy -llzma -lz
LDLIBS += $(LDLIBS_LOCAL) $(LDLIBS_AIO) $(LDLIBS_COMPRESS) $(LDLIBS_BFD)

HEADERS = $(wildcard src/*.hpp src/*.h include/*.hpp include/*.h utest/*.hpp)
BIN_SOURCES = $(wildcard binsrc/*.cpp)
OTHER_SOURCES = $(wildcard src/*.cpp)
TEST_SOURCES = $(wildcard utest/*.cpp)
SOURCES = $(OTHER_SOURCES) $(BIN_SOURCES) $(TEST_SOURCES)
BINARIES = $(patsubst %.cpp,%,$(BIN_SOURCES))
TEST_BINARIES = $(patsubst %.cpp,%,$(TEST_SOURCES))
LOCAL_LIB = src/libwalb-tools.a
NON_LIB_OBJ = $(patsubst %, src/%.o, storage storage_vol_info proxy proxy_vol_info archive archive_vol_info controller)
LOCAL_LIB_OBJ = $(filter-out $(NON_LIB_OBJ),$(patsubst %.cpp,%.o,$(OTHER_SOURCES) src/version.o src/lz4.o))
MANPAGES = $(patsubst %.ronn,%,$(wildcard man/*.ronn))

all: build
build:
	$(MAKE) version_cpp
	$(MAKE) $(BINARIES)
build-exec:
	$(MAKE) version_cpp
	$(MAKE) $(BINARIES) ENABLE_EXEC_PROTOCOL=1

core:
	$(MAKE) version_cpp
	$(MAKE) binsrc/walb-storage binsrc/walb-proxy binsrc/walb-archive binsrc/walbc binsrc/wdevc

utest: $(TEST_BINARIES)
utest_all: $(TEST_BINARIES)
	@for t in $(TEST_BINARIES); do \
	    ./$$t; \
	done 2>&1 |tee test.log |grep ^ctest:name
	@grep ctest:name test.log | grep -v "ng=0, exception=0"; \
	if [ $$? -eq 1 ]; then (echo "all unit tests succeed"; rm -f test.log test.status); else exit 1; fi

itest: $(BINARIES)
	$(MAKE) -C itest/wdiff
	$(MAKE) -C itest/wlog

echo_binaries:
	@echo $(BINARIES)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@ -MMD -MP
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@ -MMD -MP

$(LOCAL_LIB): $(LOCAL_LIB_OBJ)
	ar rv $(LOCAL_LIB) $(LOCAL_LIB_OBJ)

binsrc/walbc: binsrc/walbc.o src/controller.o $(LOCAL_LIB)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(filter %.o,$+) $(LDLIBS)

binsrc/walb-%: binsrc/walb-%.o src/%.o src/%_vol_info.o $(LOCAL_LIB)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(filter %.o,$+) $(LDLIBS)

binsrc/%: binsrc/%.o $(LOCAL_LIB)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

utest/%: utest/%.o $(LOCAL_LIB)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

clean: cleanobj cleandep cleanman
	rm -f $(BINARIES) $(TEST_BINARIES) $(LOCAL_LIB) src/version.cpp
cleanobj:
	rm -f src/*.o binsrc/*.o utest/*.o
cleandep:
	rm -f src/*.d binsrc/*.d utest/*.d
cleanman:
	rm -f $(MANPAGES)

rebuild:
	$(MAKE) clean
	$(MAKE) all

install:
	@echo not yet implemented

manpages: $(MANPAGES)

man/%: man/%.ronn
	ronn -r $<

src/version.cpp:
	$(MAKE) version_cpp

version_cpp:
	cat src/version.cpp.template |sed "s/VERSION/`cat VERSION`/g" \
|sed "s/BUILD_DATE/`date +%Y-%m-%dT%H:%M:%S`/g" \
> src/version.cpp.tmp
ifeq ($(DISABLE_COMMIT_ID),1)
	mv src/version.cpp.tmp src/version.cpp
else
	cat src/version.cpp.tmp \
|sed "s/COMMIT_ID/`git show-ref --head HEAD |cut -f 1 -d ' '|head -n1`/g" \
> src/version.cpp
	rm -f src/version.cpp.tmp
endif


PYTHON_SOURCES0 = python/walblib/__init__.py python/walblib/worker.py stest/config0.py stest/stest_util.py stest/repeater.py stest/common.py stest/scenario0.py
PYTHON_SOURCES1 = python/walblib/__init__.py python/walblib/worker.py stest/config1.py stest/stest_util.py stest/repeater.py stest/common.py stest/scenario1.py

PYLINT=pylint -E --rcfile=/dev/null -f colorized --init-hook="sys.path.insert(0, 'python')"
pylint:
	 $(PYLINT) $(PYTHON_SOURCES0)
	 $(PYLINT) $(PYTHON_SOURCES1)
	 $(PYLINT) python/walb_worker.py

stest0:
	$(MAKE) pylint
	python stest/scenario0.py $(OPT)

stest1:
	$(MAKE) pylint
	python stest/scenario1.py $(OPT)

stest100:
	$(MAKE) pylint
	python stest/scenario0.py -c 100
	python stest/scenario1.py -c 100

archive:
	git archive --format=tar master > walb-tools.tgz

utest-py:
	env PYTHONPATH=./python python utest/test.py

worker-test:
	env PYTHONPATH=./python python mtest/worker/worker-test.py

worker-itest:
	env PYTHONPATH=./python ipython mtest/worker/itest.ipy

ALL_SRC=$(BIN_SOURCES) $(OTHER_SOURCES) $(TEST_SOURCES)
DEPEND_FILE=$(ALL_SRC:.cpp=.d)
-include $(DEPEND_FILE)


# don't remove these files automatically
.SECONDARY: $(ALL_SRC:.cpp=.o)
