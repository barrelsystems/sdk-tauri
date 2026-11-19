use std::collections::HashMap;
use std::sync::Mutex;

static DEFECT_COUNTS: Mutex<Option<HashMap<&'static str, u64>>> = Mutex::new(None);

/// Tallies a bug in this SDK's own adapter code, by coarse cause, and
/// swallows it -- never panics, never leaks into `UpdateFailure`. Counts
/// only, never a message or stack trace.
pub fn record_internal_defect(cause: &'static str) {
    let mut guard = DEFECT_COUNTS.lock().unwrap();
    let counts = guard.get_or_insert_with(HashMap::new);
    *counts.entry(cause).or_insert(0) += 1;
}

/// Exposed for the future reporting path and for tests; not public API yet.
#[allow(dead_code)]
pub fn get_internal_defect_counts() -> HashMap<&'static str, u64> {
    DEFECT_COUNTS.lock().unwrap().clone().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_tallies_a_defect_by_cause() {
        // Isolated cause string so this test can't collide with another
        // test's counts under parallel execution.
        record_internal_defect("test:records_and_tallies_a_defect_by_cause");
        record_internal_defect("test:records_and_tallies_a_defect_by_cause");

        let counts = get_internal_defect_counts();
        assert_eq!(
            counts.get("test:records_and_tallies_a_defect_by_cause"),
            Some(&2)
        );
    }
}
