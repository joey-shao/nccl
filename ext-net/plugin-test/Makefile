.DEFAULT_GOAL: build

BUILDDIR ?= $(CURDIR)/build
PERF_TEST_BIN := $(BUILDDIR)/nccl-net-plugin-perf-test
SENDER_BIN := $(BUILDDIR)/nccl-net-plugin-sender
RECEIVER_BIN := $(BUILDDIR)/nccl-net-plugin-receiver

PERF_SRC := net_plugin_perf_test.cc
SENDER_SRC := sender.cc
RECEIVER_SRC := receiver.cc

CPPFLAGS += -I../example/nccl
CXXFLAGS ?= -O3 -fPIC

build: $(PERF_TEST_BIN) $(SENDER_BIN) $(RECEIVER_BIN) 

$(PERF_TEST_BIN): $(PERF_SRC)
	@printf "Compiling  %-35s > %s\n" "$<" "$@"
	@mkdir -p "$(BUILDDIR)"
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LDLIBS)

$(SENDER_BIN): $(SENDER_SRC)
	@printf "Compiling  %-35s > %s\n" "$<" "$@"
	@mkdir -p "$(BUILDDIR)"
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LDLIBS)

$(RECEIVER_BIN): $(RECEIVER_SRC)
	@printf "Compiling  %-35s > %s\n" "$<" "$@"
	@mkdir -p "$(BUILDDIR)"
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LDLIBS)

clean:
	rm -rf $(BUILDDIR)
