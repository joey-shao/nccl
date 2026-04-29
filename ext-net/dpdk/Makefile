.DEFAULT_GOAL: build

BUILDDIR ?= $(CURDIR)/build

PLUGIN_SO := $(BUILDDIR)/libnccl-net-dpdk.so

DPDK_CFLAGS ?= $(shell pkg-config --cflags libdpdk 2>/dev/null)
DPDK_LIBS ?= $(shell pkg-config --static --libs libdpdk 2>/dev/null)

ifeq ($(strip $(DPDK_CFLAGS) $(DPDK_LIBS)),)
$(warning DPDK flags are empty. Set DPDK_CFLAGS/DPDK_LIBS if build fails.)
endif

SRC_FILES := \
	net_dpdk.cc \
	net_dpdk_utils.cc \
	ncdp.cc \
	plugin_compat.cc \
	socket_compat.cc

CPPFLAGS += \
	-I. \
	-I../example/nccl \
	$(DPDK_CFLAGS)

CXXFLAGS ?= -O3 -fPIC
LDFLAGS += -shared
LDLIBS += $(DPDK_LIBS)

build: $(PLUGIN_SO)

$(PLUGIN_SO): $(SRC_FILES)
	@printf "Compiling  %-35s > %s\n" "$<" "$@"
	@mkdir -p "$(BUILDDIR)"
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

clean:
	rm -rf $(BUILDDIR)
