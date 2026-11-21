use std::path::Path;
use tauri::{AppHandle, Runtime};
use tokio::sync::Mutex;

use base64::{engine::general_purpose::STANDARD, Engine};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use pkcs8::DecodePublicKey;

use super::disk_store::DiskStore;
use crate::types::CheckInResponse;

/// Resolves, and -- when a signed rotation announcement arrives -- updates,
/// the Ed25519 public key this SDK verifies Policy responses against.
pub struct TrustedKeyStore<R: Runtime> {
    cached: Mutex<Option<String>>,
    disk: DiskStore<R>,
}

impl<R: Runtime> TrustedKeyStore<R> {
    pub fn new(app: AppHandle<R>) -> Self {
        Self {
            cached: Mutex::new(None),
            disk: DiskStore::new(app),
        }
    }

    /// Reads a previously-adopted rotation cached on disk, or falls back to
    /// the build-time default. Call once at startup, before the first
    /// check-in's Policy is verified.
    pub async fn get_or_create(&self, path: &Path, built_in_key_pem: &str) -> String {
        let mut cached = self.cached.lock().await;
        if let Some(key) = cached.as_ref() {
            return key.clone();
        }

        let key = self
            .disk
            .read(path, "publicKeyPem")
            .await
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_else(|| built_in_key_pem.to_string());
        *cached = Some(key.clone());
        key
    }

    /// The key currently in effect. `None` only if called before `get_or_create`.
    pub async fn current(&self) -> Option<String> {
        self.cached.lock().await.clone()
    }

    /// Verifies a rotation announcement against the key this SDK ALREADY
    /// trusts, never the new key -- a key can only vouch for its own
    /// successor. Any failure fails open, leaving the current key in effect.
    pub async fn apply_rotation_if_present(&self, path: &Path, response: &CheckInResponse) {
        let (Some(next_public_key_pem), Some(next_public_key_signature)) = (
            response.next_public_key_pem.as_ref(),
            response.next_public_key_signature.as_ref(),
        ) else {
            return;
        };

        // Held across the disk write too, not just the in-memory update --
        // two concurrent rotations could otherwise leave the on-disk key
        // out of sync with whichever one `cached` ends up holding.
        let mut cached = self.cached.lock().await;
        let Some(current_key_pem) = cached.clone() else {
            return;
        };

        if verify_rotation(
            &current_key_pem,
            next_public_key_pem,
            next_public_key_signature,
        )
        .is_none()
        {
            return;
        }

        *cached = Some(next_public_key_pem.clone());

        self.disk
            .write(path, "publicKeyPem", serde_json::json!(next_public_key_pem))
            .await;
    }
}

/// `Some(())` only if `next_public_key_signature` is a valid signature by
/// `current_key_pem` over `next_public_key_pem`'s own bytes, AND
/// `next_public_key_pem` itself parses as real key material.
fn verify_rotation(
    current_key_pem: &str,
    next_public_key_pem: &str,
    next_public_key_signature: &str,
) -> Option<()> {
    let current_key = VerifyingKey::from_public_key_pem(current_key_pem).ok()?;
    let signature_bytes = STANDARD.decode(next_public_key_signature).ok()?;
    let signature = Signature::from_slice(&signature_bytes).ok()?;
    current_key
        .verify(next_public_key_pem.as_bytes(), &signature)
        .ok()?;

    VerifyingKey::from_public_key_pem(next_public_key_pem).ok()?;
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::test_support::mock_app;
    use ed25519_dalek::{Signer, SigningKey};
    use pkcs8::EncodePublicKey;
    use tempfile::NamedTempFile;

    // Fixed seeds rather than `SigningKey::generate` -- deterministic, and
    // avoids pulling in a real `rand` dependency just for tests. Each
    // distinct seed byte produces a distinct, valid keypair.
    fn keypair_from_seed(seed_byte: u8) -> (SigningKey, String) {
        let signing_key = SigningKey::from_bytes(&[seed_byte; 32]);
        let pem = signing_key
            .verifying_key()
            .to_public_key_pem(pkcs8::LineEnding::LF)
            .unwrap();
        (signing_key, pem)
    }

    fn base_response(
        next_public_key_pem: Option<String>,
        next_public_key_signature: Option<String>,
    ) -> CheckInResponse {
        CheckInResponse {
            protocol_version: crate::types::PROTOCOL_VERSION,
            policy: "{}".to_string(),
            signature: "sig".to_string(),
            next_public_key_pem,
            next_public_key_signature,
        }
    }

    #[tokio::test]
    async fn get_or_create_falls_back_to_the_build_time_default_when_nothing_is_cached() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let app = mock_app();
        let store = TrustedKeyStore::new(app.handle().clone());
        let trusted = store.get_or_create(&path, "built-in-key-pem").await;
        assert_eq!(trusted, "built-in-key-pem");
    }

    #[tokio::test]
    async fn adopts_a_rotation_announcement_validly_signed_by_the_currently_trusted_key() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let (current_signing, current_pem) = keypair_from_seed(1);
        let (_, next_pem) = keypair_from_seed(2);
        let next_signature = STANDARD.encode(current_signing.sign(next_pem.as_bytes()).to_bytes());

        let app = mock_app();
        let store = TrustedKeyStore::new(app.handle().clone());
        store.get_or_create(&path, &current_pem).await;
        store
            .apply_rotation_if_present(
                &path,
                &base_response(Some(next_pem.clone()), Some(next_signature)),
            )
            .await;

        assert_eq!(store.current().await, Some(next_pem));
    }

    #[tokio::test]
    async fn fails_open_rejecting_a_rotation_signed_by_a_key_other_than_the_one_currently_trusted()
    {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let (_current_signing, current_pem) = keypair_from_seed(3);
        let (imposter_signing, _) = keypair_from_seed(4);
        let (_, next_pem) = keypair_from_seed(5);
        let next_signature = STANDARD.encode(imposter_signing.sign(next_pem.as_bytes()).to_bytes());

        let app = mock_app();
        let store = TrustedKeyStore::new(app.handle().clone());
        store.get_or_create(&path, &current_pem).await;
        store
            .apply_rotation_if_present(&path, &base_response(Some(next_pem), Some(next_signature)))
            .await;

        assert_eq!(store.current().await, Some(current_pem));
    }

    #[tokio::test]
    async fn fails_open_a_response_with_no_rotation_fields_leaves_the_current_key_untouched() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let (_signing, current_pem) = keypair_from_seed(6);
        let app = mock_app();
        let store = TrustedKeyStore::new(app.handle().clone());
        store.get_or_create(&path, &current_pem).await;
        store
            .apply_rotation_if_present(&path, &base_response(None, None))
            .await;

        assert_eq!(store.current().await, Some(current_pem));
    }

    #[tokio::test]
    async fn fails_open_malformed_next_key_material_is_rejected_even_with_a_validly_signed_envelope(
    ) {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let (current_signing, current_pem) = keypair_from_seed(7);
        let malformed_key = "not a real PEM".to_string();
        let signature = STANDARD.encode(current_signing.sign(malformed_key.as_bytes()).to_bytes());

        let app = mock_app();
        let store = TrustedKeyStore::new(app.handle().clone());
        store.get_or_create(&path, &current_pem).await;
        store
            .apply_rotation_if_present(&path, &base_response(Some(malformed_key), Some(signature)))
            .await;

        assert_eq!(store.current().await, Some(current_pem));
    }

    #[tokio::test]
    async fn persists_an_adopted_rotation_and_reloads_it_as_trusted_across_a_simulated_restart() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::remove_file(&path).unwrap();

        let (current_signing, current_pem) = keypair_from_seed(8);
        let (_, next_pem) = keypair_from_seed(9);
        let next_signature = STANDARD.encode(current_signing.sign(next_pem.as_bytes()).to_bytes());

        let first_app = mock_app();
        let first = TrustedKeyStore::new(first_app.handle().clone());
        first.get_or_create(&path, &current_pem).await;
        first
            .apply_rotation_if_present(
                &path,
                &base_response(Some(next_pem.clone()), Some(next_signature)),
            )
            .await;

        // A separate mock app -- its own `AppHandle` and resource table,
        // so no cached `Store` from `first` can leak through here. Only a
        // genuine disk read makes this pass.
        let second_app = mock_app();
        let second = TrustedKeyStore::new(second_app.handle().clone());
        let resolved = second
            .get_or_create(&path, "a-different-build-time-default")
            .await;
        assert_eq!(resolved, next_pem);
    }

    #[tokio::test]
    async fn adopts_a_rotation_in_memory_even_when_the_disk_write_is_permanently_blocked() {
        // `path`'s parent is a plain file, so `create_dir_all` can never
        // succeed -- the real shape of a blocked write, not a simulated one.
        let blocking_file = NamedTempFile::new().unwrap();
        let path = blocking_file
            .path()
            .join("subdir")
            .join("barrel-trusted-key.json");

        let (current_signing, current_pem) = keypair_from_seed(10);
        let (_, next_pem) = keypair_from_seed(11);
        let next_signature = STANDARD.encode(current_signing.sign(next_pem.as_bytes()).to_bytes());

        let app = mock_app();
        let store = TrustedKeyStore::new(app.handle().clone());
        store.get_or_create(&path, &current_pem).await;
        store
            .apply_rotation_if_present(
                &path,
                &base_response(Some(next_pem.clone()), Some(next_signature)),
            )
            .await;

        // The in-memory current key still adopts the rotation -- fail-open
        // means the *write* is best-effort, never that the rotation itself silently drops.
        assert_eq!(store.current().await, Some(next_pem));
    }
}
