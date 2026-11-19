const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
const FNV_PRIME: u32 = 0x0100_0193;

fn fnv1a(input: &str) -> u32 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in input.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Deterministic per-installation jitter fraction, fixed for the
/// installation's lifetime: magnitude 10-15%, sign derived from the same
/// hash so it never needs separate storage.
pub fn compute_jitter_fraction(installation_id: &str) -> f64 {
    let hash = fnv1a(installation_id);
    let sign: f64 = if hash & 1 == 0 { 1.0 } else { -1.0 };
    let magnitude = 0.1 + f64::from((hash >> 1) % 1000) / 1000.0 / 20.0; // 0.10..0.15
    sign * magnitude
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_deterministic_for_the_same_installation_id() {
        assert_eq!(
            compute_jitter_fraction("abc"),
            compute_jitter_fraction("abc")
        );
    }

    #[test]
    fn stays_within_the_documented_magnitude_bounds() {
        for id in ["abc", "def", "a-real-uuid-looking-string", ""] {
            let fraction = compute_jitter_fraction(id);
            assert!(
                fraction.abs() >= 0.10 && fraction.abs() <= 0.15,
                "{id}: {fraction}"
            );
        }
    }
}
