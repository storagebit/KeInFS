// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Crc32Backend {
    X86Pclmulqdq,
    Aarch64Crc,
    Software,
}

impl Crc32Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::X86Pclmulqdq => "x86-pclmulqdq",
            Self::Aarch64Crc => "aarch64-crc",
            Self::Software => "software",
        }
    }

    pub fn is_accelerated(self) -> bool {
        !matches!(self, Self::Software)
    }

    pub fn detail(self) -> &'static str {
        match self {
            Self::X86Pclmulqdq => "using x86 IEEE CRC32 acceleration via pclmulqdq + sse2 + sse4.1",
            Self::Aarch64Crc => "using aarch64 CRC instructions for IEEE CRC32",
            Self::Software => "no CRC acceleration detected; using software fallback",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KixHardwareAcceleration {
    pub cpu_arch: &'static str,
    pub crc32_backend: Crc32Backend,
}

impl KixHardwareAcceleration {
    pub fn detect() -> Self {
        Self {
            cpu_arch: cpu_arch_name(),
            crc32_backend: detect_crc32_backend(),
        }
    }

    pub fn crc32_accelerated(self) -> bool {
        self.crc32_backend.is_accelerated()
    }

    pub fn crc32_detail(self) -> &'static str {
        self.crc32_backend.detail()
    }
}

pub fn detect_hardware_acceleration() -> KixHardwareAcceleration {
    KixHardwareAcceleration::detect()
}

pub fn crc32_ieee(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

fn detect_crc32_backend() -> Crc32Backend {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("pclmulqdq")
            && std::arch::is_x86_feature_detected!("sse2")
            && std::arch::is_x86_feature_detected!("sse4.1")
        {
            return Crc32Backend::X86Pclmulqdq;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            return Crc32Backend::Aarch64Crc;
        }
    }

    Crc32Backend::Software
}

fn cpu_arch_name() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        "x86_64"
    }
    #[cfg(target_arch = "x86")]
    {
        "x86"
    }
    #[cfg(target_arch = "aarch64")]
    {
        "aarch64"
    }
    #[cfg(target_arch = "arm")]
    {
        "arm"
    }
    #[cfg(target_arch = "riscv64")]
    {
        "riscv64"
    }
    #[cfg(target_arch = "powerpc64")]
    {
        "powerpc64"
    }
    #[cfg(target_arch = "s390x")]
    {
        "s390x"
    }
    #[cfg(not(any(
        target_arch = "x86_64",
        target_arch = "x86",
        target_arch = "aarch64",
        target_arch = "arm",
        target_arch = "riscv64",
        target_arch = "powerpc64",
        target_arch = "s390x",
    )))]
    "unknown"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_wrapper_matches_known_vector() {
        assert_eq!(crc32_ieee(b"123456789"), 0xcbf4_3926);
    }

    #[test]
    fn hardware_detection_reports_a_backend() {
        let hw = detect_hardware_acceleration();
        assert!(!hw.cpu_arch.is_empty());
        assert!(!hw.crc32_backend.as_str().is_empty());
        assert!(!hw.crc32_detail().is_empty());
    }
}
