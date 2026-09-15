#[cfg(all(target_has_atomic = "32", not(feature = "std")))]
/// Generates a process-local counter ID using 32-bit atomics.
///
/// The counters reset on restart and their values are added, not concatenated;
/// IDs can repeat after wraparound. Supply your own IDs when uniqueness matters.
pub fn id() -> u128 {
    use core::sync::atomic::{AtomicU32, Ordering};

    static ID_A: AtomicU32 = AtomicU32::new(0);
    static ID_B: AtomicU32 = AtomicU32::new(0);

    // affordable risk of acq u32::max, serve 0 to another consumer so it has duplicated (0, 0),
    // then increment b
    let a = ID_A.fetch_add(1, Ordering::SeqCst);
    let b = if a == u32::MAX {
        ID_B.fetch_add(1, Ordering::SeqCst)
    } else {
        ID_B.load(Ordering::SeqCst)
    };

    a as u128 + b as u128
}

#[cfg(feature = "std")]
/// Generates a time-based UUID v7 represented as a `u128`.
pub fn id() -> u128 {
    uuid::Uuid::now_v7().as_u128()
}

#[cfg(all(not(target_has_atomic = "32"), not(feature = "std")))]
compile_error!(
    "atomic sync for u32 not supported; std not enabled; no fallback implementation for `id`"
);

#[cfg(feature = "std")]
/// Reads `OPENAI_API_DEFAULT_MODEL` into a shared optional value.
///
/// Unset or non-Unicode values become `None`. Empty strings are accepted, and
/// the returned value does not track subsequent environment changes.
pub fn get_default_model() -> std::sync::Arc<Option<String>> {
    let default_model = std::env::var("OPENAI_API_DEFAULT_MODEL").ok();
    std::sync::Arc::new(default_model)
}

#[test]
fn id_increments() {
    let a = (0..1_000).map(|_| id());
    let b = (0..1_000).map(|_| id());

    a.zip(b).for_each(|(a, b)| assert!(a < b));
}
