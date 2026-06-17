// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use std::os::raw::{c_int, c_uchar};

pub const GF_TABLE_BYTES_PER_COEFF: usize = 32;

extern "C" {
    fn ec_init_tables(k: c_int, rows: c_int, a: *const c_uchar, gftbls: *mut c_uchar);
    fn ec_encode_data(
        len: c_int,
        k: c_int,
        rows: c_int,
        gftbls: *const c_uchar,
        data: *const *const c_uchar,
        coding: *mut *mut c_uchar,
    );
    fn ec_encode_data_update(
        len: c_int,
        k: c_int,
        rows: c_int,
        vec_i: c_int,
        gftbls: *const c_uchar,
        data: *const c_uchar,
        coding: *mut *mut c_uchar,
    );
    fn gf_mul(a: c_uchar, b: c_uchar) -> c_uchar;
    fn gf_gen_rs_matrix(a: *mut c_uchar, m: c_int, k: c_int);
    fn gf_gen_cauchy1_matrix(a: *mut c_uchar, m: c_int, k: c_int);
    fn gf_invert_matrix(in_: *mut c_uchar, out: *mut c_uchar, n: c_int) -> c_int;
}

pub fn init_tables(k: usize, rows: usize, coefficients: &[u8]) -> Vec<u8> {
    assert_eq!(coefficients.len(), k * rows);
    let mut tables = vec![0_u8; GF_TABLE_BYTES_PER_COEFF * k * rows];
    unsafe {
        ec_init_tables(
            k as c_int,
            rows as c_int,
            coefficients.as_ptr(),
            tables.as_mut_ptr(),
        );
    }
    tables
}

pub fn encode_data(
    len: usize,
    k: usize,
    rows: usize,
    gftbls: &[u8],
    data: &[*const u8],
    coding: &mut [*mut u8],
) {
    assert_eq!(gftbls.len(), GF_TABLE_BYTES_PER_COEFF * k * rows);
    assert_eq!(data.len(), k);
    assert_eq!(coding.len(), rows);
    unsafe {
        ec_encode_data(
            len as c_int,
            k as c_int,
            rows as c_int,
            gftbls.as_ptr(),
            data.as_ptr(),
            coding.as_mut_ptr(),
        );
    }
}

pub fn encode_data_update(
    len: usize,
    k: usize,
    rows: usize,
    source_index: usize,
    gftbls: &[u8],
    data: *const u8,
    coding: &mut [*mut u8],
) {
    assert_eq!(gftbls.len(), GF_TABLE_BYTES_PER_COEFF * k * rows);
    assert_eq!(coding.len(), rows);
    unsafe {
        ec_encode_data_update(
            len as c_int,
            k as c_int,
            rows as c_int,
            source_index as c_int,
            gftbls.as_ptr(),
            data,
            coding.as_mut_ptr(),
        );
    }
}

pub fn generate_rs_matrix(m: usize, k: usize) -> Vec<u8> {
    let mut matrix = vec![0_u8; m * k];
    unsafe {
        gf_gen_rs_matrix(matrix.as_mut_ptr(), m as c_int, k as c_int);
    }
    matrix
}

pub fn generate_cauchy1_matrix(m: usize, k: usize) -> Vec<u8> {
    let mut matrix = vec![0_u8; m * k];
    unsafe {
        gf_gen_cauchy1_matrix(matrix.as_mut_ptr(), m as c_int, k as c_int);
    }
    matrix
}

pub fn invert_matrix_owned(mut matrix: Vec<u8>, n: usize) -> Option<Vec<u8>> {
    let mut inverted = vec![0_u8; matrix.len()];
    let rc = unsafe { gf_invert_matrix(matrix.as_mut_ptr(), inverted.as_mut_ptr(), n as c_int) };
    (rc >= 0).then_some(inverted)
}

pub fn gf_multiply(a: u8, b: u8) -> u8 {
    unsafe { gf_mul(a, b) }
}
