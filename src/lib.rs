#![cfg_attr(not(feature = "std"), no_std)]
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

extern crate alloc;

/// Task execution backends, including a test double and a `std`-only API client.
pub mod executor;
/// Producer/worker contracts and feature-gated queue backends.
pub mod queue;
/// Serializable tasks, chat inputs, responses, and builders.
pub mod task;
/// Task ID generation and environment configuration helpers.
pub mod utils;
/// Sequential loops connecting queue workers to executors.
pub mod worker;
