//! The integration tests, linked into one binary (`cargo test --test it`): one link instead of
//! one per file, and the tests of all modules run in parallel. Run one module with
//! `cargo test --test it s3::`. Process-wide state is shared by all modules, so a test that
//! mutates the process environment (`std::env::set_var`) must not live here.

mod binary;
mod cache;
mod cloud;
mod end_to_end;
mod http;
mod local;
mod oidc;
mod s3;
mod stress;
mod support;
