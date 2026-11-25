//! Shared pure helpers for Dimster cloud setup CLIs (`eks-setup`, `gke-setup`).
//!
//! Exists so both cloud CLIs share identical shell-script generation, Dimster
//! environment config rendering/insertion, Kubernetes sizing math, and
//! stateless command execution without pulling cloud-specific control flow
//! into a shared abstraction.

pub mod config_gen;
pub mod exec;
pub mod script;
pub mod sizing;
