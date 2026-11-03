//! TLS material for the REST client.
//!
//! A private CA and, for mTLS, a client certificate are selected the same way
//! as Basic Auth: a flag overrides the matching environment variable, and an
//! empty string is unset.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsPaths {
    pub cacert: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsPathError {
    MissingCert,
    MissingKey,
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|text| !text.is_empty())
}

pub fn resolve_tls_paths(
    flag_cacert: Option<&str>,
    flag_cert: Option<&str>,
    flag_key: Option<&str>,
    env_cacert: Option<&str>,
    env_cert: Option<&str>,
    env_key: Option<&str>,
) -> Result<TlsPaths, TlsPathError> {
    let cacert = nonempty(flag_cacert).or_else(|| nonempty(env_cacert));
    let cert = nonempty(flag_cert).or_else(|| nonempty(env_cert));
    let key = nonempty(flag_key).or_else(|| nonempty(env_key));
    match (cert, key) {
        (Some(_), None) => Err(TlsPathError::MissingKey),
        (None, Some(_)) => Err(TlsPathError::MissingCert),
        (cert, key) => Ok(TlsPaths {
            cacert: cacert.map(str::to_string),
            cert: cert.map(str::to_string),
            key: key.map(str::to_string),
        }),
    }
}

pub fn build_client(paths: &TlsPaths) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder();
    if let Some(path) = &paths.cacert {
        let pem = std::fs::read(path)
            .map_err(|err| format!("failed to read CA certificate {path}: {err}"))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|err| format!("invalid CA certificate {path}: {err}"))?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert_path), Some(key_path)) = (&paths.cert, &paths.key) {
        let mut pem = std::fs::read(key_path)
            .map_err(|err| format!("failed to read client key {key_path}: {err}"))?;
        if !pem.ends_with(b"\n") {
            pem.push(b'\n');
        }
        pem.extend(
            std::fs::read(cert_path)
                .map_err(|err| format!("failed to read client certificate {cert_path}: {err}"))?,
        );
        let identity = reqwest::Identity::from_pem(&pem).map_err(|err| {
            format!("invalid client certificate {cert_path} or key {key_path}: {err}")
        })?;
        builder = builder.identity(identity);
    }
    builder.build().map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_overrides_the_same_env_field_and_empty_falls_through() {
        let paths = resolve_tls_paths(
            Some("/flag/ca.crt"),
            Some(""),
            None,
            Some("/env/ca.crt"),
            Some("/env/client.crt"),
            Some("/env/client.key"),
        )
        .unwrap();
        assert_eq!(
            paths,
            TlsPaths {
                cacert: Some("/flag/ca.crt".to_string()),
                cert: Some("/env/client.crt".to_string()),
                key: Some("/env/client.key".to_string()),
            }
        );
    }

    #[test]
    fn cert_and_key_must_both_be_set() {
        assert_eq!(
            resolve_tls_paths(None, Some("client.crt"), None, None, None, None),
            Err(TlsPathError::MissingKey)
        );
        assert_eq!(
            resolve_tls_paths(None, None, Some("client.key"), None, None, None),
            Err(TlsPathError::MissingCert)
        );
    }

    #[test]
    fn no_tls_paths_build_a_client() {
        let paths = resolve_tls_paths(None, None, None, None, None, None).unwrap();
        assert!(paths.cacert.is_none() && paths.cert.is_none() && paths.key.is_none());
        assert!(build_client(&paths).is_ok());
    }

    #[test]
    fn dev_cluster_material_builds_an_mtls_client() {
        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docker/tls");
        let paths = TlsPaths {
            cacert: Some(format!("{root}/ca.crt")),
            cert: Some(format!("{root}/client.crt")),
            key: Some(format!("{root}/client.key")),
        };
        let client = build_client(&paths).expect("dev cluster PEMs should load");
        drop(client);
    }

    #[test]
    fn missing_ca_file_names_the_path() {
        let paths = TlsPaths {
            cacert: Some("/no/such/ca.crt".to_string()),
            cert: None,
            key: None,
        };
        let err = build_client(&paths).unwrap_err();
        assert!(err.contains("/no/such/ca.crt"), "{err}");
        assert!(!err.contains("BEGIN"));
    }
}
