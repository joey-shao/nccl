#
# Copyright (c) 2015-2022, NVIDIA CORPORATION. All rights reserved.
#
# See LICENSE.txt for license information
#
include ../makefiles/common.mk
include ../makefiles/version.mk

##### src files
INCEXPORTS  := nccl.h nccl_device.h \
	$(patsubst include/%,%,$(wildcard include/nccl_device/*.h include/nccl_device/*/*.h include/nccl_device/*/*/*.h))

LIBSRCFILES := \
	bootstrap.cc channel.cc collectives.cc debug.cc enqueue.cc group.cc \
	init.cc init_nvtx.cc proxy.cc transport.cc mnnvl.cc allocator.cc dev_runtime.cc sym_kernels.cc ce_coll.cc \
	$(wildcard graph/*.cc) \
	$(wildcard misc/*.cc) \
	$(wildcard transport/*.cc) \
	$(wildcard transport/gdaki/*.cc) \
	$(wildcard register/*.cc) \
	$(wildcard plugin/*.cc) \
	$(wildcard plugin/net/*.cc) \
	$(wildcard plugin/tuner/*.cc) \
	$(wildcard plugin/profiler/*.cc) \
	$(wildcard plugin/env/*.cc) \
	$(wildcard nccl_device/*.cc) \
	$(wildcard scheduler/*.cc) \
	$(wildcard gin/*.cc) \
	$(filter-out ras/client.cc,$(wildcard ras/*.cc))
BINSRCFILES := ras/client.cc

NETTESTFILES := $(wildcard transport/net_plugin_test/*.cc) 

NETTEST_IMPL ?= socket
DPDK_CFLAGS ?= $(shell pkg-config --cflags libdpdk 2>/dev/null)
DPDK_LIBS   ?= $(shell pkg-config --libs libdpdk 2>/dev/null)

ifeq ($(NETTEST_IMPL),dpdk)
NETTESTSRCFILES := $(NETTESTFILES) transport/net_dpdk.cc transport/ncdp.cc misc/socket.cc
NETTESTCXXFLAGS := -DNETTEST_USE_DPDK $(DPDK_CFLAGS)
NETTESTLDFLAGS := $(DPDK_LIBS)
ifneq ($(strip $(DPDK_CFLAGS) $(DPDK_LIBS)),)
# DPDK flags are provided via pkg-config or user overrides.
else
$(warning NETTEST_IMPL=dpdk but DPDK flags are empty. Set DPDK_CFLAGS/DPDK_LIBS if build fails.)
endif
else ifeq ($(NETTEST_IMPL),socket)
NETTESTSRCFILES := $(NETTESTFILES) transport/net_socket.cc misc/socket.cc
NETTESTCXXFLAGS :=
NETTESTLDFLAGS :=
else
$(error Unsupported NETTEST_IMPL='$(NETTEST_IMPL)'. Use NETTEST_IMPL=socket or NETTEST_IMPL=dpdk)
endif

##### lib files
LIBNAME     := libnccl.so
STATICLIBNAME := libnccl_static.a
##### binaries
BINNAME := ncclras
NETTESTBINNAME := ncclnettest
##### pkgconfig files
PKGCONFIGFILE := nccl.pc
##### dirs
BUILDDIR ?= $(abspath ../build)
INCDIR := $(BUILDDIR)/include
LIBDIR := $(BUILDDIR)/lib
OBJDIR := $(BUILDDIR)/obj
PKGDIR := $(BUILDDIR)/lib/pkgconfig
BINDIR := $(BUILDDIR)/bin

##### target files
CUDARTLIB  ?= cudart_static

# Use compatibility shim only with static cudart; see https://github.com/NVIDIA/nccl/issues/658
ifeq ($(CUDARTLIB), cudart_static)
	LIBSRCFILES += enhcompat.cc
endif

# Keep transport net integration test sources out of libnccl.so objects.
LIBSRCFILES := $(filter-out $(NETTESTFILES) ,$(LIBSRCFILES))

INCTARGETS := $(INCEXPORTS:%=$(INCDIR)/%)
LIBSONAME  := $(LIBNAME:%=%.$(NCCL_MAJOR))
LIBTARGET  := $(LIBNAME:%=%.$(NCCL_MAJOR).$(NCCL_MINOR).$(NCCL_PATCH))
STATICLIBTARGET := $(STATICLIBNAME)
PKGTARGET  := $(PKGCONFIGFILE)
LIBOBJ     := $(LIBSRCFILES:%.cc=$(OBJDIR)/%.o)
BINOBJ     := $(BINSRCFILES:%.cc=$(OBJDIR)/%.o)
NETTESTOBJ := $(NETTESTSRCFILES:%.cc=$(OBJDIR)/%.o)
DEPFILES   := $(LIBOBJ:%.o=%.d) $(BINOBJ:%.o=%.d) $(NETTESTOBJ:%.o=%.d)
LDFLAGS    += -L${CUDA_LIB} -l$(CUDARTLIB) -lpthread -lrt -ldl
INCPLUGIN  := include/plugin

DEVMANIFEST := $(BUILDDIR)/obj/device/manifest

# DOCA GPUNetIO definitions
DOCA_HOME        ?= transport/gdaki/doca-gpunetio
DOCA_INC_INSTALL := $(INCDIR)/nccl_device/gin/gdaki/doca_gpunetio
DOCA_OBJDIR      := $(OBJDIR)/transport/gdaki/doca-gpunetio
DOCA_INCLUDES    := $(DOCA_HOME)/include/doca_gpunetio_device.h $(wildcard $(DOCA_HOME)/include/common/*.h) $(wildcard $(DOCA_HOME)/include/device/*.cuh)
DOCA_INCTARGETS  := $(DOCA_INCLUDES:$(DOCA_HOME)/include/%=$(DOCA_INC_INSTALL)/%)
INCTARGETS       += $(DOCA_INCTARGETS)
DOCA_LIBSRC      := doca_verbs_qp.cpp doca_verbs_cq.cpp doca_verbs_device_attr.cpp doca_verbs_umem.cpp doca_verbs_srq.cpp doca_verbs_uar.cpp doca_gpunetio.cpp doca_gpunetio_log.cpp doca_gpunetio_high_level.cpp doca_verbs_cuda_wrapper.cpp doca_verbs_mlx5dv_wrapper.cpp doca_verbs_ibv_wrapper.cpp doca_gpunetio_gdrcopy.cpp
DOCA_LIBOBJ      := $(DOCA_LIBSRC:%.cpp=$(DOCA_OBJDIR)/%.o)
LIBOBJ           += $(DOCA_LIBOBJ)

##### rules
build : lib staticlib binary

lib : $(INCTARGETS) $(LIBDIR)/$(LIBTARGET) $(PKGDIR)/$(PKGTARGET)

staticlib : $(LIBDIR)/$(STATICLIBTARGET)

binary : $(BINDIR)/$(BINNAME)

nettest : $(BINDIR)/$(NETTESTBINNAME)

$(DEVMANIFEST): ALWAYS_REBUILD $(INCTARGETS)
	$(MAKE) -C ./device

# Empty target to force rebuild
ALWAYS_REBUILD:

-include $(DEPFILES)
$(LIBDIR)/$(LIBTARGET) $(LIBDIR)/$(STATICLIBTARGET) : $(LIBOBJ)

$(INCDIR)/nccl.h : nccl.h.in ../makefiles/version.mk
# NCCL_VERSION(X,Y,Z) ((X) * 10000 + (Y) * 100 + (Z))
	@$(eval NCCL_VERSION := $(shell printf "%d%02d%02d" $(NCCL_MAJOR) $(NCCL_MINOR) $(NCCL_PATCH)))
	mkdir -p $(INCDIR)
	@printf "Generating %-35s > %s\n" $< $@
	sed -e "s/\$${nccl:Major}/$(NCCL_MAJOR)/g" \
	    -e "s/\$${nccl:Minor}/$(NCCL_MINOR)/g" \
	    -e "s/\$${nccl:Patch}/$(NCCL_PATCH)/g" \
	    -e "s/\$${nccl:Suffix}/$(NCCL_SUFFIX)/g" \
	    -e "s/\$${nccl:Version}/$(NCCL_VERSION)/g" \
	    $< > $@

$(LIBDIR)/$(LIBTARGET): $(LIBOBJ) $(DEVMANIFEST)
	@printf "Linking    %-35s > %s\n" $(LIBTARGET) $@
	mkdir -p $(LIBDIR)
	$(CXX) $(CXXFLAGS) -shared -Wl,--no-as-needed -Wl,-soname,$(LIBSONAME) -o $@ $(LIBOBJ) $$(cat $(DEVMANIFEST)) $(LDFLAGS) -Wl,--version-script=libnccl.map
	ln -sf $(LIBSONAME) $(LIBDIR)/$(LIBNAME)
	ln -sf $(LIBTARGET) $(LIBDIR)/$(LIBSONAME)

$(LIBDIR)/$(STATICLIBTARGET): $(LIBOBJ) $(DEVMANIFEST)
	@printf "Archiving  %-35s > %s\n" $(STATICLIBTARGET) $@
	mkdir -p $(LIBDIR)
	ar cr $@ $(LIBOBJ) $$(cat $(DEVMANIFEST))

$(BINDIR)/$(BINNAME): $(BINOBJ)
	@printf "Linking    %-35s > %s\n" $(BINNAME) $@
	mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(BINDIR)/$(NETTESTBINNAME): $(NETTESTOBJ)
	@printf "Linking    %-35s > %s\n" $(NETTESTBINNAME) $@
	mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $(NETTESTOBJ) $(LDFLAGS) $(NETTESTLDFLAGS) -o $@

$(NETTESTOBJ): CXXFLAGS += $(NETTESTCXXFLAGS)

$(PKGDIR)/nccl.pc : nccl.pc.in
	mkdir -p $(PKGDIR)
	@printf "Generating %-35s > %s\n" $< $@
	sed -e 's|$${nccl:Prefix}|\$(PREFIX)|g' \
	    -e "s/\$${nccl:Major}/$(NCCL_MAJOR)/g" \
	    -e "s/\$${nccl:Minor}/$(NCCL_MINOR)/g" \
	    -e "s/\$${nccl:Patch}/$(NCCL_PATCH)/g" \
	    $< > $@

$(INCDIR)/%.h : %.h
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(INCDIR)
	install -m 644 $< $@

$(INCDIR)/nccl_%.h : include/nccl_%.h
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(INCDIR)
	install -m 644 $< $@

$(INCDIR)/nccl_device/%.h: include/nccl_device/%.h
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(INCDIR)/nccl_device
	install -m 644 $< $@

$(INCDIR)/nccl_device/impl/%.h: include/nccl_device/impl/%.h
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(INCDIR)/nccl_device/impl
	install -m 644 $< $@

$(INCDIR)/nccl_device/gin/%.h: include/nccl_device/gin/%.h
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(INCDIR)/nccl_device/gin
	install -m 644 $< $@

$(INCDIR)/nccl_device/gin/gdaki/%.h: include/nccl_device/gin/gdaki/%.h
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(INCDIR)/nccl_device/gin/gdaki
	install -m 644 $< $@

$(INCDIR)/nccl_device/gin/proxy/%.h: include/nccl_device/gin/proxy/%.h
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(INCDIR)/nccl_device/gin/proxy
	install -m 644 $< $@

$(DOCA_INC_INSTALL)/%.h: $(DOCA_HOME)/include/%.h
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(DOCA_INC_INSTALL)
	install -m 644 $< $@

$(DOCA_INC_INSTALL)/common/%.h: $(DOCA_HOME)/include/common/%.h
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(DOCA_INC_INSTALL)/common
	install -m 644 $< $@

$(DOCA_INC_INSTALL)/device/%.cuh: $(DOCA_HOME)/include/device/%.cuh
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(DOCA_INC_INSTALL)/device
	install -m 644 $< $@

$(PKGDIR)/%.pc : %.pc
	@printf "Grabbing   %-35s > %s\n" $< $@
	mkdir -p $(PKGDIR)
	install -m 644 $< $@

$(OBJDIR)/%.o : %.cc $(INCTARGETS)
	@printf "Compiling  %-35s > %s\n" $< $@
	mkdir -p `dirname $@`
	$(CXX) -I. -I$(INCDIR) $(CXXFLAGS) -Iinclude -I$(INCPLUGIN) -I$(DOCA_HOME)/include -c $< -o $@
	@$(CXX) -I. -I$(INCDIR) $(CXXFLAGS) -Iinclude -I$(INCPLUGIN) -I$(DOCA_HOME)/include -M $< > $(@:%.o=%.d.tmp)
	@sed "0,/^.*:/s//$(subst /,\/,$@):/" $(@:%.o=%.d.tmp) > $(@:%.o=%.d)
	@sed -e 's/.*://' -e 's/\\$$//' < $(@:%.o=%.d.tmp) | fmt -1 | \
                sed -e 's/^ *//' -e 's/$$/:/' >> $(@:%.o=%.d)
	@rm -f $(@:%.o=%.d.tmp)

$(DOCA_OBJDIR)/%.o : $(DOCA_HOME)/src/%.cpp
	@printf "Compiling  %-35s > %s\n" $< $@
	mkdir -p `dirname $@`
	$(CXX) -I$(DOCA_HOME)/src -I$(DOCA_HOME)/include $(CXXFLAGS) -c $< -o $@
	@$(CXX) -I$(DOCA_HOME)/src -I$(DOCA_HOME)/include $(CXXFLAGS) -M $< > $(@:%.o=%.d.tmp)
	@sed "0,/^.*:/s//$(subst /,\/,$@):/" $(@:%.o=%.d.tmp) > $(@:%.o=%.d)
	@sed -e 's/.*://' -e 's/\\$$//' < $(@:%.o=%.d.tmp) | fmt -1 | \
                sed -e 's/^ *//' -e 's/$$/:/' >> $(@:%.o=%.d)
	@rm -f $(@:%.o=%.d.tmp)

clean :
	$(MAKE) -C device clean
	rm -rf ${BINDIR} ${INCDIR} ${LIBDIR} ${PKGDIR} ${OBJDIR}

install : build
	mkdir -p $(PREFIX)/lib
	mkdir -p $(PREFIX)/lib/pkgconfig
	mkdir -p $(PREFIX)/include
	mkdir -p $(PREFIX)/bin
	cp -P -v $(BUILDDIR)/lib/lib* $(PREFIX)/lib/
	cp -P -v $(BUILDDIR)/lib/pkgconfig/* $(PREFIX)/lib/pkgconfig/
	cp -v -r $(BUILDDIR)/include/* $(PREFIX)/include/
	cp -v $(BUILDDIR)/bin/ncclras $(PREFIX)/bin/

FILESTOFORMAT := $(shell find . -name ".\#*" -prune -o \( -name "*.cc" -o -name "*.h" \) -print | grep -v -E 'ibvwrap.h|nvmlwrap.h|gdrwrap.h|nccl.h')
# Note that formatting.mk defines a new target so in order to not overwrite the default target,
# it shouldn't be included at the top. Also, it uses the above definition of FILESTOFORMAT as well
# as the BUILDDIR variable.
include ../makefiles/formatting.mk
