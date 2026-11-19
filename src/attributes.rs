use std::collections::HashMap;

const MAX_ATTRIBUTE_KEYS: usize = 20;
const MAX_ATTRIBUTE_VALUE_BYTES: usize = 256;

/// Enforces the wire protocol's bounds: at most 20 keys, each value ≤256 UTF-8 bytes.
pub fn bound_attributes(attributes: &HashMap<String, String>) -> HashMap<String, String> {
    attributes
        .iter()
        .take(MAX_ATTRIBUTE_KEYS)
        .map(|(key, value)| {
            (
                key.clone(),
                truncate_to_byte_limit(value, MAX_ATTRIBUTE_VALUE_BYTES),
            )
        })
        .collect()
}

fn truncate_to_byte_limit(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }

    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_at_most_twenty_keys() {
        let mut attributes = HashMap::new();
        for i in 0..30 {
            attributes.insert(format!("key{i}"), "value".to_string());
        }
        assert_eq!(bound_attributes(&attributes).len(), 20);
    }

    #[test]
    fn truncates_a_value_to_the_byte_limit_without_splitting_a_codepoint() {
        let mut attributes = HashMap::new();
        attributes.insert("k".to_string(), "é".repeat(200)); // 2 bytes each -> 400 bytes
        let bounded = bound_attributes(&attributes);
        let value = bounded.get("k").unwrap();
        assert!(value.len() <= 256);
        assert!(value.is_char_boundary(value.len()));
    }
}
