use reqwest::RequestBuilder;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialError {
    MissingUsername,
    MissingPassword,
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|text| !text.is_empty())
}

pub fn resolve_credentials(
    flag_username: Option<&str>,
    flag_password: Option<&str>,
    env_username: Option<&str>,
    env_password: Option<&str>,
) -> Result<Option<Credentials>, CredentialError> {
    let username = nonempty(flag_username).or_else(|| nonempty(env_username));
    let password = nonempty(flag_password).or_else(|| nonempty(env_password));
    match (username, password) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(CredentialError::MissingPassword),
        (None, Some(_)) => Err(CredentialError::MissingUsername),
        (Some(username), Some(password)) => Ok(Some(Credentials {
            username: username.to_string(),
            password: password.to_string(),
        })),
    }
}

pub fn apply_basic_auth(builder: RequestBuilder, creds: Option<&Credentials>) -> RequestBuilder {
    match creds {
        Some(creds) => builder.basic_auth(&creds.username, Some(&creds.password)),
        None => builder,
    }
}

pub fn format_http_result(
    status: reqwest::StatusCode,
    body: &Value,
    sent_auth: bool,
) -> Result<String, String> {
    let detail = body["error"]
        .as_str()
        .or_else(|| body["message"].as_str())
        .unwrap_or("");
    let line = if detail.is_empty() {
        format!("[{}]", status.as_u16())
    } else {
        format!("[{}] {detail}", status.as_u16())
    };
    if status.is_success() {
        return Ok(line);
    }
    if status.as_u16() == 401 && !sent_auth {
        return Err(format!(
            "{line}\nset --username and --password, or PG_HA_USERNAME and PG_HA_PASSWORD"
        ));
    }
    Err(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;
    use serde_json::json;

    fn creds(user: &str, pass: &str) -> Credentials {
        Credentials {
            username: user.to_string(),
            password: pass.to_string(),
        }
    }

    #[test]
    fn flag_overrides_the_same_env_field() {
        let resolved = resolve_credentials(
            Some("flag-user"),
            None,
            Some("env-user"),
            Some("env-pass"),
        )
        .unwrap();
        assert_eq!(resolved, Some(creds("flag-user", "env-pass")));
    }

    #[test]
    fn both_empty_means_no_credentials() {
        assert_eq!(resolve_credentials(None, None, None, None).unwrap(), None);
        assert_eq!(
            resolve_credentials(Some(""), Some(""), Some(""), Some("")).unwrap(),
            None
        );
    }

    #[test]
    fn one_side_is_an_error() {
        assert_eq!(
            resolve_credentials(Some("alice"), None, None, None),
            Err(CredentialError::MissingPassword)
        );
        assert_eq!(
            resolve_credentials(None, Some("secret"), None, None),
            Err(CredentialError::MissingUsername)
        );
    }

    #[test]
    fn request_carries_basic_auth_when_credentials_exist() {
        let client = reqwest::Client::new();
        let request = apply_basic_auth(
            client.post("http://127.0.0.1:8008/switchover"),
            Some(&creds("alice", "secret")),
        )
        .build()
        .unwrap();
        let header = request
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        // base64("alice:secret")
        assert_eq!(header, "Basic YWxpY2U6c2VjcmV0");
    }

    #[test]
    fn request_omits_authorization_when_credentials_are_absent() {
        let client = reqwest::Client::new();
        let request = apply_basic_auth(client.post("http://127.0.0.1:8008/switchover"), None)
            .build()
            .unwrap();
        assert!(request.headers().get(reqwest::header::AUTHORIZATION).is_none());
    }

    #[test]
    fn unauthorized_without_credentials_names_the_flags() {
        let err = format_http_result(StatusCode::UNAUTHORIZED, &json!({"error": "Unauthorized"}), false)
            .unwrap_err();
        assert_eq!(
            err,
            "[401] Unauthorized\nset --username and --password, or PG_HA_USERNAME and PG_HA_PASSWORD"
        );
    }

    #[test]
    fn unauthorized_with_credentials_has_no_hint() {
        let err = format_http_result(StatusCode::UNAUTHORIZED, &json!({"error": "Unauthorized"}), true)
            .unwrap_err();
        assert_eq!(err, "[401] Unauthorized");
    }

    #[test]
    fn success_uses_message_and_failure_prefers_error() {
        let ok = format_http_result(StatusCode::OK, &json!({"message": "switched"}), true).unwrap();
        assert_eq!(ok, "[200] switched");
        let err = format_http_result(
            StatusCode::BAD_REQUEST,
            &json!({"error": "nope", "message": "ignored"}),
            true,
        )
        .unwrap_err();
        assert_eq!(err, "[400] nope");
    }
}
