# SPDX-License-Identifier: GPL-2.0-or-later
# Copyright (C) 2026 Andreas Krause / storagebit

.SHELLFLAGS := -e -o pipefail -c
SHELL := bash
.DEFAULT_GOAL := help

ROOT_DIR := $(CURDIR)
BUILD_DIR ?= $(ROOT_DIR)/build
CONFIG_MK := $(BUILD_DIR)/config.mk
CONFIG_ENV := $(BUILD_DIR)/config.env

ifneq ($(wildcard $(CONFIG_MK)),)
include $(CONFIG_MK)
endif

export CARGO_TARGET_DIR

BUILD_PROFILE ?= release
PROFILE_DIR := $(if $(filter $(BUILD_PROFILE),release),release,debug)
PROFILE_FLAG := $(if $(filter $(BUILD_PROFILE),release),--release,)
ENABLE_FUSE ?= 1
ENABLE_ISA_L ?= 0
ENABLE_KIX_BENCH ?= 0
ROOT_DIR ?= $(CURDIR)
CARGO ?= cargo
RENDER_TOOL := $(ROOT_DIR)/tools/keinfs-tooling.sh

RENDER_ROOT := $(BUILD_DIR)/render
RENDER_CONFIG_ROOT := $(RENDER_ROOT)/etc/keinfs
RENDER_SYSTEMD_ROOT := $(RENDER_ROOT)/systemd
SKIP_FDB_CHECK ?= 0
SKIP_ISA_L_CHECK ?= 0
UNAME_S := $(shell uname -s 2>/dev/null || echo unknown)

KSC_FEATURES :=
ifeq ($(ENABLE_ISA_L),1)
KSC_FEATURES += --features isa-l-backend
endif

KRS_FEATURES :=
ifeq ($(ENABLE_ISA_L),1)
KRS_FEATURES += --features isa-l-backend
endif

KFC_FEATURES :=
ifeq ($(ENABLE_FUSE),1)
KFC_FEATURES += --features fuse
endif

.PHONY: help
help:
	@printf '%s\n' \
	  'KeInFS root tooling' \
	  '' \
	  'First run ./configure, then use:' \
	  '  make build                    Build the current binary set' \
	  '  make render-configs           Render generic config examples' \
	  '  make render-systemd           Render generic systemd units' \
	  '  make render                   Render configs and units' \
	  '  make install                  Install binaries, helper scripts, configs, and units' \
	  '  make show-config              Print the active configure summary'

.PHONY: ensure-config
ensure-config:
	@if [[ ! -f "$(CONFIG_MK)" || ! -f "$(CONFIG_ENV)" ]]; then \
	  echo "missing $(CONFIG_MK) or $(CONFIG_ENV); run ./configure first" >&2; \
	  exit 1; \
	fi

.PHONY: show-config
show-config: ensure-config
	@cat "$(BUILD_DIR)/config.toml"

.PHONY: build
build: ensure-config preflight-build
	"$(CARGO)" build --manifest-path "$(ROOT_DIR)/poc/keinctl/Cargo.toml" --bin keinctl $(PROFILE_FLAG)
	"$(CARGO)" build --manifest-path "$(ROOT_DIR)/poc/kms/Cargo.toml" --bin kms $(PROFILE_FLAG)
	"$(CARGO)" build --manifest-path "$(ROOT_DIR)/poc/kas/Cargo.toml" --bin kas $(PROFILE_FLAG)
	"$(CARGO)" build --manifest-path "$(ROOT_DIR)/poc/krs/Cargo.toml" --bin krs $(KRS_FEATURES) $(PROFILE_FLAG)
	"$(CARGO)" build --manifest-path "$(ROOT_DIR)/poc/ksc/Cargo.toml" --bin ksc $(KSC_FEATURES) $(PROFILE_FLAG)
	"$(CARGO)" build --manifest-path "$(ROOT_DIR)/poc/kst/Cargo.toml" --bin kst $(PROFILE_FLAG)
	"$(CARGO)" build --manifest-path "$(ROOT_DIR)/poc/keinexport/Cargo.toml" --bin keinexport $(PROFILE_FLAG)
ifeq ($(ENABLE_FUSE),1)
	"$(CARGO)" build --manifest-path "$(ROOT_DIR)/poc/kfc/Cargo.toml" --bin kfc $(KFC_FEATURES) $(PROFILE_FLAG)
endif
ifeq ($(ENABLE_KIX_BENCH),1)
	"$(CARGO)" build --manifest-path "$(ROOT_DIR)/poc/kix/Cargo.toml" --package kix-bench --bin kix-bench $(PROFILE_FLAG)
endif

.PHONY: preflight-build
preflight-build:
ifeq ($(UNAME_S),Linux)
ifeq ($(SKIP_FDB_CHECK),0)
	@if ! command -v ldconfig >/dev/null 2>&1; then \
	  echo "warning: ldconfig is not available, so the FoundationDB client library preflight was skipped" >&2; \
	elif ! ldconfig -p 2>/dev/null | grep -q 'libfdb_c\.so'; then \
	  echo "missing FoundationDB client library (libfdb_c). Install the FoundationDB client/runtime package before building kms/kas on Linux." >&2; \
	  exit 1; \
	fi
endif
ifeq ($(ENABLE_ISA_L),1)
ifeq ($(SKIP_ISA_L_CHECK),0)
	@if ! command -v ldconfig >/dev/null 2>&1; then \
	  echo "warning: ldconfig is not available, so the Intel ISA-L preflight was skipped" >&2; \
	elif ! ldconfig -p 2>/dev/null | grep -q 'libisal\.so'; then \
	  echo "missing Intel ISA-L (libisal). Install isa-l before building the ISA-L backend, or re-run ./configure --disable-isa-l to use the slow software fallback (not for production)." >&2; \
	  exit 1; \
	fi
endif
endif
endif

.PHONY: render-configs
render-configs: ensure-config
	"$(RENDER_TOOL)" render-configs --config-env "$(CONFIG_ENV)" --out-dir "$(RENDER_CONFIG_ROOT)"

.PHONY: render-systemd
render-systemd: ensure-config
	"$(RENDER_TOOL)" render-systemd --config-env "$(CONFIG_ENV)" --out-dir "$(RENDER_SYSTEMD_ROOT)"

.PHONY: render
render: render-configs render-systemd

.PHONY: install
install: build render
	install -d "$(DESTDIR)$(BINDIR)"
	install -m 0755 "$(CARGO_TARGET_DIR)/$(PROFILE_DIR)/keinctl" "$(DESTDIR)$(BINDIR)/keinctl"
	install -m 0755 "$(CARGO_TARGET_DIR)/$(PROFILE_DIR)/kms" "$(DESTDIR)$(BINDIR)/kms"
	install -m 0755 "$(CARGO_TARGET_DIR)/$(PROFILE_DIR)/kas" "$(DESTDIR)$(BINDIR)/kas"
	install -m 0755 "$(CARGO_TARGET_DIR)/$(PROFILE_DIR)/krs" "$(DESTDIR)$(BINDIR)/krs"
	install -m 0755 "$(CARGO_TARGET_DIR)/$(PROFILE_DIR)/ksc" "$(DESTDIR)$(BINDIR)/ksc"
	install -m 0755 "$(CARGO_TARGET_DIR)/$(PROFILE_DIR)/kst" "$(DESTDIR)$(BINDIR)/kst"
	install -m 0755 "$(CARGO_TARGET_DIR)/$(PROFILE_DIR)/keinexport" "$(DESTDIR)$(BINDIR)/keinexport"
ifeq ($(ENABLE_FUSE),1)
	install -m 0755 "$(CARGO_TARGET_DIR)/$(PROFILE_DIR)/kfc" "$(DESTDIR)$(BINDIR)/kfc"
endif
ifeq ($(ENABLE_KIX_BENCH),1)
	install -m 0755 "$(CARGO_TARGET_DIR)/$(PROFILE_DIR)/kix-bench" "$(DESTDIR)$(BINDIR)/kix-bench"
endif
	install -d "$(DESTDIR)$(LIBEXECDIR)"
	install -m 0755 "$(ROOT_DIR)/tools/kst-runner.sh" "$(DESTDIR)$(LIBEXECDIR)/kst-runner"
	install -d "$(DESTDIR)$(SYSCONFDIR)/keinfs/examples"
	cp -R "$(RENDER_CONFIG_ROOT)/." "$(DESTDIR)$(SYSCONFDIR)/keinfs/examples/"
	install -d "$(DESTDIR)$(SYSTEMD_UNIT_DIR)"
	cp -R "$(RENDER_SYSTEMD_ROOT)/." "$(DESTDIR)$(SYSTEMD_UNIT_DIR)/"

.PHONY: clean
clean:
	rm -rf "$(BUILD_DIR)"
