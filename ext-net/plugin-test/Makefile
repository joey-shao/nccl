.DEFAULT_GOAL: build

BUILDDIR ?= $(CURDIR)/build
PERF_TEST_BIN := $(BUILDDIR)/nccl-net-plugin-perf-test

PERF_SRC := net_plugin_perf_test.cc

CPPFLAGS += -I../example/nccl
CXXFLAGS ?= -O3 -fPIC

build: $(PERF_TEST_BIN)

$(PERF_TEST_BIN): $(PERF_SRC)
	@printf "Compiling  %-35s > %s\n" "$<" "$@"
	@mkdir -p "$(BUILDDIR)"
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LDLIBS)

clean:
	rm -rf $(BUILDDIR)
