//! In-cluster mTLS provisioning for `full` deploy mode.
//!
//! Generates a self-signed CA + broker cert + client cert (all pure-Rust via
//! `rcgen`, output as PEM) and packages them into two namespaced Kubernetes
//! Secrets:
//!
//!   - `kafka-tls-broker` — broker key/cert + CA cert. Mounted into the
//!     kafka StatefulSet.
//!   - `kafka-tls-client` — client key/cert + CA cert. Mounted into worker
//!     + coordinator pods so any workload can opt in to TLS at run time.
//!
//! The whole thing is **idempotent**: if both Secrets already exist in the
//! namespace we leave them untouched, so previously-issued client material
//! keeps working across redeploys. `dimster teardown` deletes the namespace
//! and so removes the Secrets — at that point a fresh CA is generated next
//! time around.
//!
//! We use Kafka's native `ssl.keystore.type=PEM` support (available since
//! Kafka 2.7) which lets us skip the PKCS12/JKS dance entirely. Each
//! "keystore" is just a single PEM file containing the certificate followed
//! by the private key; each "truststore" is a single PEM file containing the
//! CA certificate.
//!
//! The PEM private keys are written **unencrypted**. Kafka's PEM keystore
//! parser interprets the presence of `ssl.key.password` as "the key is
//! encrypted, decrypt it with this" — passing a password against an
//! unencrypted key fails with `IOException: overrun, bytes = ...`. So we
//! simply omit the password property on both broker and client. Confidentiality
//! of the key material is gated by Kubernetes Secret RBAC, which is the same
//! trust boundary that would have protected the password anyway.

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use time::{Duration, OffsetDateTime};

use crate::kubectl::Kubectl;

pub const BROKER_SECRET: &str = "kafka-tls-broker";
pub const CLIENT_SECRET: &str = "kafka-tls-client";

/// Mount path inside broker pods. Kept stable so changes to ConfigMap
/// templates don't drift from the deploy-time mount.
pub const BROKER_MOUNT_PATH: &str = "/etc/kafka/tls";
/// Mount path inside worker + coordinator pods. The injected
/// ssl.keystore.location / ssl.truststore.location values point here.
pub const CLIENT_MOUNT_PATH: &str = "/etc/dimster/tls";

/// Filenames inside the Secrets — same on both sides for symmetry. The
/// "keystore" PEM holds cert+key; the "truststore" PEM holds the CA.
pub const KEYSTORE_FILE: &str = "keystore.pem";
pub const TRUSTSTORE_FILE: &str = "truststore.pem";

/// Cert validity in days. 10 years is long enough to never expire during a
/// benchmark campaign and matches the throwaway nature of the cluster.
const VALIDITY_DAYS: u32 = 3650;

/// Ensure the broker + client Secrets exist in the namespace. Idempotent:
/// when both Secrets already exist we leave them in place so previously-
/// issued client material keeps working across redeploys.
///
/// `replicas` and `namespace` are used to derive the SANs on the broker
/// cert: every `kafka-N.kafka-headless.<ns>.svc.cluster.local`, plus the
/// `kafka-bootstrap.<ns>.svc.cluster.local` Service and its short alias.
pub fn ensure_secrets(kubectl: &Kubectl, replicas: u32, namespace: &str) -> Result<()> {
    let broker_exists = secret_exists(kubectl, BROKER_SECRET)?;
    let client_exists = secret_exists(kubectl, CLIENT_SECRET)?;

    if broker_exists && client_exists {
        // Pre-mTLS-fix Secrets had a third key, `password`, that we no longer
        // generate. Detect that shape and force regeneration so brokers /
        // clients stop seeing a stale ssl.key.password reference (see file-
        // level comment for why a password against an unencrypted PEM fails).
        if has_legacy_password_key(kubectl, CLIENT_SECRET)
            || has_legacy_password_key(kubectl, BROKER_SECRET)
        {
            eprintln!(
                "    Detected legacy TLS secrets (password file present) — regenerating without password"
            );
            let _ = kubectl.run(&["delete", "secret", BROKER_SECRET, "--ignore-not-found"]);
            let _ = kubectl.run(&["delete", "secret", CLIENT_SECRET, "--ignore-not-found"]);
        } else {
            println!(
                "    Reusing existing TLS secrets ({}, {})",
                BROKER_SECRET, CLIENT_SECRET
            );
            return Ok(());
        }
    } else if broker_exists != client_exists {
        // If only one exists we delete it before regenerating — keeping a half-
        // applied pair around would mean broker and clients trust different CAs.
        eprintln!(
            "    WARNING: only one of {} / {} found — regenerating both",
            BROKER_SECRET, CLIENT_SECRET
        );
        if broker_exists {
            let _ = kubectl.run(&["delete", "secret", BROKER_SECRET, "--ignore-not-found"]);
        }
        if client_exists {
            let _ = kubectl.run(&["delete", "secret", CLIENT_SECRET, "--ignore-not-found"]);
        }
    }

    println!("    Generating self-signed CA and broker/client certs...");
    let bundle = generate_bundle(replicas, namespace)?;

    create_secret(
        kubectl,
        BROKER_SECRET,
        &[
            (KEYSTORE_FILE, bundle.broker_keystore_pem.as_bytes()),
            (TRUSTSTORE_FILE, bundle.truststore_pem.as_bytes()),
        ],
    )?;

    create_secret(
        kubectl,
        CLIENT_SECRET,
        &[
            (KEYSTORE_FILE, bundle.client_keystore_pem.as_bytes()),
            (TRUSTSTORE_FILE, bundle.truststore_pem.as_bytes()),
        ],
    )?;

    println!(
        "    Created TLS secrets ({}, {})",
        BROKER_SECRET, CLIENT_SECRET
    );
    Ok(())
}

/// True when the Secret carries a `password` data key from a previous
/// (pre-fix) deploy. Used to force a regenerate so the broker pods don't
/// keep referencing `ssl.keystore.key.password.file=.../password`.
fn has_legacy_password_key(kubectl: &Kubectl, name: &str) -> bool {
    kubectl
        .run(&["get", "secret", name, "-o", "jsonpath={.data.password}"])
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
}

/// Bundle of generated cert material. Internal — callers see it as two
/// Secrets via [`ensure_secrets`]; this struct just keeps the generation
/// step easy to test in isolation.
struct CertBundle {
    /// Broker cert + key concatenated as PEM, ready for
    /// `ssl.keystore.type=PEM` + `ssl.keystore.location=...`. The private
    /// key block is unencrypted — Kafka must NOT be given an
    /// `ssl.key.password` for this keystore (see file-level comment).
    broker_keystore_pem: String,
    /// Client cert + key concatenated as PEM (private key unencrypted).
    client_keystore_pem: String,
    /// CA cert as PEM, used as the truststore by both sides.
    truststore_pem: String,
}

fn generate_bundle(replicas: u32, namespace: &str) -> Result<CertBundle> {
    // ---- CA ----
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name = dn("dimster-ca");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params.not_before = time_now();
    ca_params.not_after = time_after_days(VALIDITY_DAYS);

    let ca_key = KeyPair::generate().context("Generating CA key")?;
    let ca_cert = ca_params.self_signed(&ca_key).context("Self-signing CA")?;

    // ---- Broker cert ----
    // SANs cover every broker pod's stable headless DNS name plus the
    // bootstrap Service. We add a wildcard for the headless domain so adding
    // brokers later (without regenerating certs) continues to work.
    let mut sans: Vec<String> = Vec::new();
    for i in 0..replicas {
        sans.push(format!(
            "kafka-{}.kafka-headless.{}.svc.cluster.local",
            i, namespace
        ));
    }
    sans.push(format!("*.kafka-headless.{}.svc.cluster.local", namespace));
    sans.push(format!("kafka-bootstrap.{}.svc.cluster.local", namespace));
    sans.push("kafka-bootstrap".to_string());
    sans.push("kafka".to_string());
    sans.push("localhost".to_string());

    let mut broker_params = CertificateParams::new(sans).context("Building broker SAN list")?;
    broker_params.distinguished_name = dn("kafka-broker");
    broker_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    broker_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        // Inter-broker connections re-use the broker cert for client auth
        // when interBroker: true is set, so include ClientAuth too.
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    broker_params.not_before = time_now();
    broker_params.not_after = time_after_days(VALIDITY_DAYS);

    let broker_key = KeyPair::generate().context("Generating broker key")?;
    let broker_cert = broker_params
        .signed_by(&broker_key, &ca_cert, &ca_key)
        .context("Signing broker cert")?;

    // ---- Client cert ----
    // The client CN doubles as the Kafka principal for ACL purposes. We
    // hard-code "dimster-client" — benchmarking doesn't use ACLs and a
    // stable name makes broker logs easier to read.
    let mut client_params = CertificateParams::new(vec!["dimster-client".to_string()])
        .context("Building client SAN list")?;
    client_params.distinguished_name = dn("dimster-client");
    client_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    client_params.not_before = time_now();
    client_params.not_after = time_after_days(VALIDITY_DAYS);

    let client_key = KeyPair::generate().context("Generating client key")?;
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .context("Signing client cert")?;

    // Concatenate cert+key into one PEM each. Kafka's PEM keystore reads
    // both blocks from the same file — order doesn't matter, but cert-then-
    // key matches what `keytool -exportcert | openssl pkey` would produce.
    let broker_keystore_pem = format!("{}{}", broker_cert.pem(), broker_key.serialize_pem());
    let client_keystore_pem = format!("{}{}", client_cert.pem(), client_key.serialize_pem());
    let truststore_pem = ca_cert.pem();

    Ok(CertBundle {
        broker_keystore_pem,
        client_keystore_pem,
        truststore_pem,
    })
}

fn dn(common_name: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, common_name);
    dn.push(DnType::OrganizationName, "Dimster");
    dn
}

fn time_now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

fn time_after_days(days: u32) -> OffsetDateTime {
    OffsetDateTime::now_utc() + Duration::days(days as i64)
}

fn secret_exists(kubectl: &Kubectl, name: &str) -> Result<bool> {
    // `kubectl get secret <name>` returns non-zero if absent. We swallow the
    // error and treat any failure as "not present" — a real cluster outage
    // surfaces in the next kubectl call anyway.
    Ok(kubectl
        .run(&["get", "secret", name, "--ignore-not-found", "-o", "name"])
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false))
}

/// Create a generic Secret from a list of (filename, contents) entries.
/// We pipe each value through stdin via repeated `--from-literal` would not
/// work for binary content, so we shell out to a temp file dance: write
/// each file, run `kubectl create secret generic --from-file=...`, clean
/// up. The temp dir auto-cleans on Drop.
fn create_secret(kubectl: &Kubectl, name: &str, entries: &[(&str, &[u8])]) -> Result<()> {
    let dir = tempfile::tempdir().context("Creating tempdir for secret material")?;
    let mut from_file_args: Vec<String> = Vec::new();
    for (filename, content) in entries {
        let path = dir.path().join(filename);
        std::fs::write(&path, content)
            .with_context(|| format!("Writing temp secret file {}", filename))?;
        from_file_args.push(format!("--from-file={}={}", filename, path.display()));
    }

    // Build &[&str] for kubectl.run. Layout: create secret generic <name> --from-file=... ...
    let mut args: Vec<&str> = vec!["create", "secret", "generic", name];
    let arg_refs: Vec<&str> = from_file_args.iter().map(|s| s.as_str()).collect();
    args.extend(arg_refs);

    kubectl
        .run(&args)
        .with_context(|| format!("kubectl create secret {}", name))?;
    Ok(())
}

/// Lines to append to `server.properties` to enable the SSL listener (and
/// optionally inter-broker SSL). Returned as a Vec so the caller can join
/// with `|` for the entrypoint env-var transport.
///
/// `ssl_port` is the TCP port for the new SSL listener. `inter_broker` flips
/// `inter.broker.listener.name` from PLAINTEXT to SSL.
///
/// No `ssl.key.password` / `ssl.keystore.key.password.file` is emitted — the
/// PEM private key in `BROKER_SECRET` is unencrypted and Kafka would
/// otherwise try (and fail) to decrypt it. See file-level comment.
pub fn broker_tls_lines(ssl_port: u16, inter_broker: bool) -> Vec<String> {
    let inter_listener = if inter_broker { "SSL" } else { "PLAINTEXT" };
    vec![
        format!(
            "listeners=PLAINTEXT://:9092,SSL://:{},CONTROLLER://:9093",
            ssl_port
        ),
        format!(
            "advertised.listeners=PLAINTEXT://${{ADVERTISED_HOSTNAME}}:9092,SSL://${{ADVERTISED_HOSTNAME}}:{}",
            ssl_port
        ),
        "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL,CONTROLLER:PLAINTEXT"
            .to_string(),
        format!("inter.broker.listener.name={}", inter_listener),
        "ssl.client.auth=required".to_string(),
        "ssl.keystore.type=PEM".to_string(),
        format!(
            "ssl.keystore.location={}/{}",
            BROKER_MOUNT_PATH, KEYSTORE_FILE
        ),
        "ssl.truststore.type=PEM".to_string(),
        format!(
            "ssl.truststore.location={}/{}",
            BROKER_MOUNT_PATH, TRUSTSTORE_FILE
        ),
    ]
}

/// Key/value pairs that should land in a Kafka client `commonConfig` map when
/// a workload opts in to TLS via `security.protocol: SSL`. Returned as
/// (key, value) so the run command can drop them straight into a
/// `serde_yaml::Mapping` without doing string parsing.
///
/// `bootstrap.servers` is **not** included here — the caller fills it in with
/// the SSL-port bootstrap list (different per cluster size). `security.protocol`
/// is also omitted: it's the user-supplied trigger, so it's already present.
/// `ssl.key.password` is also omitted (see file-level comment) — the PEM
/// private key in `CLIENT_SECRET` is unencrypted.
pub fn client_tls_pairs() -> Vec<(&'static str, String)> {
    vec![
        ("ssl.keystore.type", "PEM".to_string()),
        (
            "ssl.keystore.location",
            format!("{}/{}", CLIENT_MOUNT_PATH, KEYSTORE_FILE),
        ),
        ("ssl.truststore.type", "PEM".to_string()),
        (
            "ssl.truststore.location",
            format!("{}/{}", CLIENT_MOUNT_PATH, TRUSTSTORE_FILE),
        ),
    ]
}

/// The bootstrap list of Dimster's in-cluster brokers in `namespace` on
/// `port` (9092 PLAINTEXT, or the SSL port). Per-broker FQDNs, so it resolves
/// from any namespace, including an `external-env` env's.
pub fn bootstrap_servers(replicas: u32, namespace: &str, port: u16) -> String {
    (0..replicas)
        .map(|i| {
            format!(
                "kafka-{}.kafka-headless.{}.svc.cluster.local:{}",
                i, namespace, port
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pure-Rust round-trip — no kubectl involvement. Verifies the bundle
    // contains the expected PEM blocks and that the password is non-empty.
    // The actual TLS handshake is exercised in the e2e verification step.
    #[test]
    fn generate_bundle_produces_pem_blocks() {
        let bundle = generate_bundle(3, "dimster").expect("bundle");
        assert!(bundle.broker_keystore_pem.contains("BEGIN CERTIFICATE"));
        assert!(bundle.broker_keystore_pem.contains("BEGIN PRIVATE KEY"));
        assert!(bundle.client_keystore_pem.contains("BEGIN CERTIFICATE"));
        assert!(bundle.client_keystore_pem.contains("BEGIN PRIVATE KEY"));
        assert!(bundle.truststore_pem.contains("BEGIN CERTIFICATE"));
        // The PEM private key MUST be unencrypted — passing
        // ssl.key.password against an encrypted key is what the broker /
        // AdminClient parser used to choke on. If rcgen ever starts
        // emitting `ENCRYPTED PRIVATE KEY` blocks, this test fails fast.
        assert!(!bundle.broker_keystore_pem.contains("ENCRYPTED PRIVATE KEY"));
        assert!(!bundle.client_keystore_pem.contains("ENCRYPTED PRIVATE KEY"));
    }

    #[test]
    fn broker_tls_lines_inter_broker_toggle() {
        let off = broker_tls_lines(9094, false);
        assert!(
            off.iter()
                .any(|l| l == "inter.broker.listener.name=PLAINTEXT")
        );

        let on = broker_tls_lines(9094, true);
        assert!(on.iter().any(|l| l == "inter.broker.listener.name=SSL"));
    }

    #[test]
    fn broker_tls_lines_emit_no_password() {
        // Regression: an `ssl.keystore.key.password.file=...` line on the
        // broker pointed at a `password` Secret entry and made the SSL
        // factory try to decrypt the (unencrypted) PEM key on listener bind.
        for line in broker_tls_lines(9094, false) {
            assert!(
                !line.contains("password"),
                "unexpected password line: {}",
                line
            );
        }
    }

    #[test]
    fn client_tls_pairs_includes_paths_and_no_password() {
        let pairs = client_tls_pairs();
        // Locations should reference the worker/coordinator mount path
        let keystore = pairs.iter().find(|(k, _)| *k == "ssl.keystore.location");
        assert!(keystore.is_some(), "missing ssl.keystore.location");
        assert!(keystore.unwrap().1.starts_with(CLIENT_MOUNT_PATH));
        // PEM type for both keystore and truststore (Kafka 2.7+ native PEM support)
        assert!(
            pairs
                .iter()
                .any(|(k, v)| *k == "ssl.keystore.type" && v == "PEM")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| *k == "ssl.truststore.type" && v == "PEM")
        );
        // Regression: must not emit ssl.key.password — Kafka treats the
        // presence of that key as "decrypt the PEM private key" and our
        // PEM key is unencrypted.
        assert!(!pairs.iter().any(|(k, _)| *k == "ssl.key.password"));
    }

    #[test]
    fn bootstrap_servers_lists_each_broker_on_the_port() {
        let bs = bootstrap_servers(2, "dimster", 9094);
        assert_eq!(
            bs,
            "kafka-0.kafka-headless.dimster.svc.cluster.local:9094,\
             kafka-1.kafka-headless.dimster.svc.cluster.local:9094"
        );
    }
}
