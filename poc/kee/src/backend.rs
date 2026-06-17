// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    IsaL,
    Software,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardwareInventory {
    pub cpu_arch: String,
    pub isa_l_feature_enabled: bool,
    pub isa_l_runtime_available: bool,
    pub primary_backend: BackendKind,
    pub fallback_backend: BackendKind,
    pub selected_backend: BackendKind,
    pub detail: String,
}

pub fn hardware_inventory() -> HardwareInventory {
    let isa_l_feature_enabled = cfg!(feature = "isa-l-backend");
    let selected_backend = if isa_l_feature_enabled {
        BackendKind::IsaL
    } else {
        BackendKind::Software
    };
    HardwareInventory {
        cpu_arch: std::env::consts::ARCH.to_string(),
        isa_l_feature_enabled,
        isa_l_runtime_available: isa_l_feature_enabled,
        primary_backend: BackendKind::IsaL,
        fallback_backend: BackendKind::Software,
        selected_backend,
        detail: if isa_l_feature_enabled {
            "isa-l backend compiled in; current selected backend is ISA-L".to_string()
        } else {
            "isa-l backend not compiled in; using software Reed-Solomon fallback".to_string()
        },
    }
}

pub fn backend_inventory() -> HardwareInventory {
    hardware_inventory()
}

/// Emits a one-time, loud stderr warning when the slow software Reed-Solomon
/// backend is active. ISA-L is the intended production backend; silently
/// running the scalar fallback would make benchmarks and SLAs meaningless.
/// Suppressed inside `kee`'s own unit tests.
pub(crate) fn maybe_warn_software_backend() {
    if cfg!(test) {
        return;
    }
    if hardware_inventory().selected_backend != BackendKind::Software {
        return;
    }
    static WARNED: std::sync::Once = std::sync::Once::new();
    WARNED.call_once(|| {
        eprintln!(
            "\n\
             ============================================================\n\
             WARNING: KeInFS erasure coding is running the SOFTWARE\n\
             Reed-Solomon backend (scalar galois_8). This is a fallback\n\
             and is several times slower than Intel ISA-L.\n\
             Rebuild with ISA-L: ./configure --enable-isa-l && make build\n\
             (or `cargo ... --features isa-l-backend`). Do NOT publish\n\
             performance numbers from this build.\n\
             ============================================================"
        );
    });
}
