//! Shared in-process task/response queues; cloning shares the same storage.
//!
//! Both backends correlate responses by task ID, so IDs must be unique among
//! outstanding tasks. With `std`, bounded Tokio channels wait for work and
//! apply backpressure. Without `std`, spin-locked queues poll immediately and
//! discard old entries when an optional capacity is reached.

#[cfg(feature = "std")]
/// Tokio-backed queue; defaults to a capacity of 100 for each channel.
pub type MemoryQueue = use_std::MemoryQueueStd;

#[cfg(not(feature = "std"))]
/// Polling, spin-locked queue; unbounded unless a capacity is supplied.
pub type MemoryQueue = nostd::MemoryQueueNoStd;

mod nostd;
#[cfg(feature = "std")]
mod use_std;

#[cfg(test)]
mod tests;
