use base64::{engine::general_purpose::STANDARD, Engine};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use pkcs8::DecodePublicKey;

use crate::types::{CheckInResponse, Policy};

/// Verifies the raw `policy` byte string's Ed25519 signature, then parses
/// it. Any failure fails open, returning `None`.
pub fn verify_and_parse_policy(response: &CheckInResponse, public_key_pem: &str) -> Option<Policy> {
    let public_key = VerifyingKey::from_public_key_pem(public_key_pem).ok()?;
    let signature_bytes = STANDARD.decode(&response.signature).ok()?;
    let signature = Signature::from_slice(&signature_bytes).ok()?;
    public_key
        .verify(response.policy.as_bytes(), &signature)
        .ok()?;

    serde_json::from_str(&response.policy).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use pkcs8::EncodePublicKey;

    fn keypair() -> (SigningKey, String) {
        let signing_key = SigningKey::from_bytes(&[42; 32]);
        let pem = signing_key
            .verifying_key()
            .to_public_key_pem(pkcs8::LineEnding::LF)
            .unwrap();
        (signing_key, pem)
    }

    fn response_signed_by(signing_key: &SigningKey, policy_json: &str) -> CheckInResponse {
        let signature = STANDARD.encode(signing_key.sign(policy_json.as_bytes()).to_bytes());
        CheckInResponse {
            protocol_version: crate::types::PROTOCOL_VERSION,
            policy: policy_json.to_string(),
            signature,
            next_public_key_pem: None,
            next_public_key_signature: None,
        }
    }

    #[test]
    fn a_validly_signed_current_policy_parses() {
        let (signing_key, pem) = keypair();
        let response = response_signed_by(
            &signing_key,
            r#"{"type":"current","issuedAt":"2026-01-01T00:00:00.000Z","environment":"production"}"#,
        );

        assert_eq!(
            verify_and_parse_policy(&response, &pem),
            Some(Policy::Current {
                issued_at: "2026-01-01T00:00:00.000Z".to_string(),
                environment: "production".to_string(),
            })
        );
    }

    #[test]
    fn a_policy_signed_by_a_different_key_than_the_one_verified_against_is_rejected() {
        let (signing_key, _) = keypair();
        let (_, other_pem) = keypair_other();
        let response = response_signed_by(
            &signing_key,
            r#"{"type":"current","issuedAt":"2026-01-01T00:00:00.000Z","environment":"production"}"#,
        );

        assert_eq!(verify_and_parse_policy(&response, &other_pem), None);
    }

    #[test]
    fn a_tampered_policy_body_fails_verification_even_with_a_structurally_valid_signature() {
        let (signing_key, pem) = keypair();
        let mut response = response_signed_by(
            &signing_key,
            r#"{"type":"current","issuedAt":"2026-01-01T00:00:00.000Z","environment":"production"}"#,
        );
        response.policy =
            r#"{"type":"current","issuedAt":"2099-01-01T00:00:00.000Z","environment":"production"}"#
                .to_string();

        assert_eq!(verify_and_parse_policy(&response, &pem), None);
    }

    #[test]
    fn malformed_policy_json_fails_open_even_when_the_signature_is_valid() {
        let (signing_key, pem) = keypair();
        let response = response_signed_by(&signing_key, "not json");

        assert_eq!(verify_and_parse_policy(&response, &pem), None);
    }

    fn keypair_other() -> (SigningKey, String) {
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let pem = signing_key
            .verifying_key()
            .to_public_key_pem(pkcs8::LineEnding::LF)
            .unwrap();
        (signing_key, pem)
    }
}
