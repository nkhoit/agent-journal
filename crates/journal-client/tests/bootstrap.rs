use journal_client::journal_protocol::*;
use journal_client::{Client, ClientError};

struct Recorder;
impl Transport for Recorder {
    fn send(&self, request: Request) -> Result<Response, TransportError> {
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/admin/principals");
        assert!(!request.headers.contains_key("Authorization"));
        let body: PrincipalCreateRequest = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body.handle, "alice");
        Ok(Response::new(
            403,
            br#"{"error":{"code":"denied","message":"secret must not escape","request_id":"r1"}}"#
                .to_vec(),
        ))
    }
}

#[test]
fn typed_admin_request_and_redacted_failure() {
    let error = Client::new(Recorder)
        .create_principal(&PrincipalCreateRequest {
            handle: "alice".into(),
            display_name: "Alice".into(),
        })
        .unwrap_err();
    assert!(matches!(error, ClientError::Http { status: 403 }));
    assert!(!error.to_string().contains("secret"));
}

struct ProfileRecorder;

const PROFILE_TEST_TOKEN: &str = concat!(
    "aaaaaaaaaaaaaaaa",
    "aaaaaaaaaaaaaaaa",
    "aaaaaaaaaaaaaaaa",
    "aaaaaaaaaaaaaaaa",
);

impl Transport for ProfileRecorder {
    fn send(&self, request: Request) -> Result<Response, TransportError> {
        assert_eq!(request.method, "PATCH");
        assert_eq!(request.path, "/v1/me/profile");
        assert_eq!(
            request.headers.get("Authorization"),
            Some(&format!("Bearer {PROFILE_TEST_TOKEN}"))
        );
        assert_eq!(
            request.headers.get("Idempotency-Key"),
            Some(&"profile-key".into())
        );
        let body: ProfileUpdateRequest = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body.handle, "renamed");
        Ok(Response::new(
            200,
            br#"{"id":"018f1f59-6e90-7000-8000-000000000001","handle":"renamed","display_name":"Renamed","profile_revision":2,"created_at":"2026-01-01T00:00:00Z","disabled":false}"#
                .to_vec(),
        ))
    }
}

#[test]
fn profile_update_is_authenticated_idempotent_and_uuid_native() {
    let principal = Client::new(ProfileRecorder)
        .update_profile(
            PROFILE_TEST_TOKEN,
            "profile-key",
            &ProfileUpdateRequest {
                handle: "renamed".into(),
                display_name: "Renamed".into(),
                description: None,
                expected_profile_revision: 1,
            },
        )
        .unwrap();
    assert_eq!(principal.id, "018f1f59-6e90-7000-8000-000000000001");
    assert_eq!(principal.profile_revision, 2);
}

#[test]
fn insecure_remote_endpoint_is_rejected() {
    assert!(journal_client::HttpTransport::new("http://example.com").is_err());
    assert!(journal_client::HttpTransport::new("https://user:password@example.com").is_err());
}

struct MutationRecorder {
    path: &'static str,
}

impl Transport for MutationRecorder {
    fn send(&self, request: Request) -> Result<Response, TransportError> {
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, self.path);
        assert!(!request.headers.contains_key("Authorization"));
        Ok(Response::new(204, Vec::new()))
    }
}

#[test]
fn recovery_and_revocation_use_only_admin_paths() {
    Client::new(MutationRecorder {
        path: "/v1/admin/credentials/revoke",
    })
    .revoke(&CredentialRotateRequest {
        credential_id: "credential-1".into(),
        reason: None,
    })
    .unwrap();
    Client::new(MutationRecorder {
        path: "/v1/admin/enrollment/recover",
    })
    .recover_enrollment(&journal_client::EnrollmentRecoveryRequest {
        adapter_id: "adapter-1".into(),
        instance_id: "installation-1".into(),
    })
    .unwrap();
}

#[test]
fn rotation_requires_secret_and_redacts_debug() {
    assert!(serde_json::from_str::<journal_client::RotationResponse>(
        r#"{"metadata":{"credential_id":"old","principal_id":"alice","class":"principal-client","rotated_at":"2026-01-01T00:00:00Z"}}"#
    ).is_err());
    let secret = journal_client::ReplacementSecret {
        credential_id: "replacement".into(),
        secret: "fake-secret".into(),
    };
    assert!(!format!("{secret:?}").contains("fake-secret"));
}

struct JsonReply(&'static str);
impl Transport for JsonReply {
    fn send(&self, _: Request) -> Result<Response, TransportError> {
        Ok(Response::new(200, self.0.as_bytes().to_vec()))
    }
}

#[test]
fn client_rejects_positional_response_arrays() {
    let client = Client::new(JsonReply(r#"["adapter-example","principal-example"]"#));
    assert!(matches!(
        client.provision_adapter(&AdapterProvisionRequest {
            adapter_id: "adapter-example".into(),
            principal_id: "principal-example".into(),
        }),
        Err(ClientError::Json)
    ));
}

#[test]
fn client_rejects_duplicate_response_keys_and_accepts_typed_objects() {
    let input = AdapterProvisionRequest {
        adapter_id: "adapter-example".into(),
        principal_id: "principal-example".into(),
    };
    let duplicate = Client::new(JsonReply(
        r#"{"adapter_id":"adapter-example","adapter_id":"other","principal_id":"principal-example"}"#,
    ));
    assert!(matches!(
        duplicate.provision_adapter(&input),
        Err(ClientError::Json)
    ));
    let duplicate_unknown = Client::new(JsonReply(
        r#"{"adapter_id":"adapter-example","principal_id":"principal-example","extra":0,"extra":1}"#,
    ));
    assert!(matches!(
        duplicate_unknown.provision_adapter(&input),
        Err(ClientError::Json)
    ));
    let valid = Client::new(JsonReply(
        r#"{"adapter_id":"adapter-example","principal_id":"principal-example"}"#,
    ));
    let response = valid.provision_adapter(&input).unwrap();
    assert_eq!(response.adapter_id, input.adapter_id);
    assert_eq!(response.principal_id, input.principal_id);
}
