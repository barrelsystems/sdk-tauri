/// Ed25519 public verification key, embedded at build time -- public by
/// design, only its private half is secret. Matches `DEFAULT_BASE_URL`
/// below specifically; a dev/test stage signs with a different keypair.
pub const PUBLIC_ED25519_KEY_PEM: &str = "-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAoQf7qhQlr7W9x6EvTwriO/EluLP1IWbn77nFRkxXKZ4=
-----END PUBLIC KEY-----
";

pub const DEFAULT_BASE_URL: &str = "https://ingest.barrel.codes";
