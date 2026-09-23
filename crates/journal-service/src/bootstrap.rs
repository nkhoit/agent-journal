//! Principal registration, authentication and protected administration.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use journal_domain::{Principal, Space, default_limits};
use journal_protocol::*;
use journal_storage_sqlite::{Database, StorageError};
use rusqlite::{OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::Clock;

#[path = "inbox.rs"]
mod inbox;
#[path = "records.rs"]
mod records;

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("idempotency key was used for another payload")]
    IdempotencyConflict,
    #[error("invalid journal request")]
    InvalidJournal,
    #[error("invalid persisted journal data")]
    CorruptJournal,
    #[error("invalid request: {0}")]
    Invalid(#[from] WireValidationError),
    #[error("invalid or expired credential")]
    Unauthorized,
    #[error("resource not found")]
    NotFound,
    #[error("operation conflicts with existing state")]
    Conflict,
    #[error("storage operation failed")]
    Storage(#[from] StorageError),
    #[error("database operation failed")]
    Sqlite(#[source] rusqlite::Error),
    #[error("secure random source failed")]
    Random,
    #[error("clock is outside supported range")]
    Clock,
    #[error("injected transaction failure")]
    Injected,
}

impl From<rusqlite::Error> for BootstrapError {
    fn from(error: rusqlite::Error) -> Self {
        if matches!(&error, rusqlite::Error::SqliteFailure(e, _) if e.code == rusqlite::ErrorCode::ConstraintViolation)
        {
            Self::Conflict
        } else {
            Self::Sqlite(error)
        }
    }
}

pub trait SecretSource: Send + Sync {
    fn fill(&self, bytes: &mut [u8; 32]) -> Result<(), BootstrapError>;
}

pub struct OsSecretSource;
impl SecretSource for OsSecretSource {
    fn fill(&self, bytes: &mut [u8; 32]) -> Result<(), BootstrapError> {
        getrandom::fill(bytes).map_err(|_| BootstrapError::Random)
    }
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedCredential {
    pub credential_id: String,
    pub principal_id: String,
    pub class: CredentialClass,
}

pub type CredentialRotation = CredentialRotationResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationOutcome {
    pub receipt: RegistrationReceipt,
    pub replayed: bool,
}

struct ExistingRegistration {
    principal_id: String,
    unrevoked: bool,
    unexpired: bool,
    request_json: Option<String>,
    response_json: Option<String>,
}

#[derive(Clone, Copy)]
enum ReadIdentity<'a> {
    Bearer(&'a str),
    ConfiguredViewer(&'a str),
}

/// Host-configured read-only authority. Never construct from request data.
pub struct SharedViewer<'a> {
    service: &'a BootstrapService,
    principal: &'a str,
}

impl SharedViewer<'_> {
    pub fn record(&self, id: &str) -> Result<journal_domain::Record, BootstrapError> {
        self.service
            .get_record_as(ReadIdentity::ConfiguredViewer(self.principal), id)
    }

    pub fn spaces(&self, query: &PageQuery) -> Result<SpacePage, BootstrapError> {
        self.service
            .list_spaces_as(ReadIdentity::ConfiguredViewer(self.principal), query)
    }

    pub fn records(
        &self,
        space: &str,
        query: &ListRecordsQuery,
    ) -> Result<RecordPage, BootstrapError> {
        self.service
            .list_records_as(ReadIdentity::ConfiguredViewer(self.principal), space, query)
    }

    pub fn search(
        &self,
        space: &str,
        query: &SearchRecordsQuery,
    ) -> Result<SearchPage, BootstrapError> {
        self.service
            .search_records_as(ReadIdentity::ConfiguredViewer(self.principal), space, query)
    }

    pub fn thread(&self, id: &str, query: &PageQuery) -> Result<RecordPage, BootstrapError> {
        self.service
            .get_thread_as(ReadIdentity::ConfiguredViewer(self.principal), id, query)
    }

    /// Root record of the thread containing `id`, for viewer title display.
    /// Returns the record plus whether its reply-to chain resolved to a
    /// genuine root; on fallback the title must not be used as thread title.
    /// Internal helper; not a public endpoint.
    pub fn thread_root(&self, id: &str) -> Result<(journal_domain::Record, bool), BootstrapError> {
        self.service
            .get_thread_root_as(ReadIdentity::ConfiguredViewer(self.principal), id)
    }

    /// Batch thread root IDs and titles for viewer breadcrumbs.
    /// Internal helper; not a public endpoint.
    pub fn thread_roots(
        &self,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, (String, Option<String>)>, BootstrapError> {
        self.service
            .get_thread_roots_as(ReadIdentity::ConfiguredViewer(self.principal), ids)
    }

    pub fn delivery(
        &self,
        id: &str,
        query: &PageQuery,
    ) -> Result<ReceiptStatusPage, BootstrapError> {
        self.service
            .delivery_status_as(ReadIdentity::ConfiguredViewer(self.principal), id, query)
    }
}

#[derive(Clone)]
pub struct BootstrapService {
    database: Database,
    clock: Arc<dyn Clock + Send + Sync>,
    random: Arc<dyn SecretSource>,
    principal_sequence: Arc<AtomicU64>,
    failpoint: Option<&'static str>,
}

impl BootstrapService {
    pub fn operational_metrics(
        &self,
    ) -> Result<journal_protocol::OperationalMetrics, BootstrapError> {
        let sampled_at = self.now()?;
        let snapshot = self.database.operational_snapshot()?;
        let recovery = self.database.recovery_status()?;
        Ok(journal_protocol::OperationalMetrics {
            sampled_at,
            database_bytes: snapshot.database_bytes,
            wal_bytes: snapshot.wal_bytes,
            unacknowledged_inbox_count: snapshot.unacknowledged_inbox_count,
            oldest_unacknowledged_at: snapshot.oldest_unacknowledged_at,
            last_backup_at: recovery.last_backup_at,
            last_verified_restore_at: recovery.last_verified_restore_at,
        })
    }

    pub fn shared_viewer<'a>(&'a self, principal: &'a str) -> SharedViewer<'a> {
        SharedViewer {
            service: self,
            principal,
        }
    }

    fn read_actor(
        &self,
        tx: &Transaction<'_>,
        identity: ReadIdentity<'_>,
    ) -> Result<String, BootstrapError> {
        match identity {
            ReadIdentity::Bearer(token) => self.journal_actor(tx, token),
            ReadIdentity::ConfiguredViewer(principal) => {
                journal_domain::validate_identifier("viewer", principal)
                    .map_err(|_| BootstrapError::InvalidJournal)?;
                active_principal(tx, principal)
            }
        }
    }

    pub fn new(database: Database) -> Self {
        Self::with_sources(database, Arc::new(SystemClock), Arc::new(OsSecretSource))
    }

    pub fn with_sources(
        database: Database,
        clock: Arc<dyn Clock + Send + Sync>,
        random: Arc<dyn SecretSource>,
    ) -> Self {
        Self {
            database,
            clock,
            random,
            principal_sequence: Arc::new(AtomicU64::new(0)),
            failpoint: None,
        }
    }

    /// Deterministic fault injection for proving rollback at durable boundaries.
    pub fn with_failpoint(mut self, boundary: &'static str) -> Self {
        self.failpoint = Some(boundary);
        self
    }

    fn checkpoint(&self, boundary: &str) -> Result<(), BootstrapError> {
        if self.failpoint == Some(boundary) {
            Err(BootstrapError::Injected)
        } else {
            Ok(())
        }
    }

    fn now(&self) -> Result<String, BootstrapError> {
        timestamp(self.clock.now())
    }

    fn secret(&self) -> Result<String, BootstrapError> {
        let mut bytes = [0; 32];
        self.random.fill(&mut bytes)?;
        Ok(hex(&bytes))
    }

    fn principal_id(&self) -> Result<String, BootstrapError> {
        let millis = self
            .clock
            .now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| BootstrapError::Clock)?
            .as_millis();
        if millis >= (1_u128 << 48) {
            return Err(BootstrapError::Clock);
        }
        let mut random = [0; 32];
        self.random.fill(&mut random)?;
        let mut bytes: [u8; 16] = random[..16].try_into().expect("fixed size");
        let sequence = self.principal_sequence.fetch_add(1, Ordering::Relaxed);
        bytes[..6].copy_from_slice(&(millis as u64).to_be_bytes()[2..]);
        for (byte, sequence_byte) in bytes[8..].iter_mut().zip(sequence.to_be_bytes()) {
            *byte ^= sequence_byte;
        }
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let value = hex(&bytes);
        Ok(format!(
            "{}-{}-{}-{}-{}",
            &value[..8],
            &value[8..12],
            &value[12..16],
            &value[16..20],
            &value[20..]
        ))
    }

    fn transaction<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, BootstrapError>,
    ) -> Result<T, BootstrapError> {
        self.database.with_transaction_for(operation)
    }

    pub fn create_principal(
        &self,
        request: &PrincipalCreateRequest,
    ) -> Result<Principal, BootstrapError> {
        request.validate()?;
        let now = self.now()?;
        let id = self.principal_id()?;
        self.transaction(|tx| {
            tx.execute(
                "INSERT INTO principals(id,display_name,created_at) VALUES (?,?,?)",
                params![id, request.display_name, now],
            )?;
            tx.execute(
                "INSERT INTO principal_names(name,principal_id,kind,created_at) VALUES (?,?,'current',?)",
                params![request.handle, id, now],
            )?;
            Ok(())
        })?;
        Ok(Principal {
            id,
            handle: request.handle.clone(),
            display_name: request.display_name.clone(),
            description: None,
            profile_revision: 1,
            created_at: now,
            disabled: false,
        })
    }

    pub fn register(
        &self,
        token: &str,
        request: &RegistrationRequest,
    ) -> Result<RegistrationOutcome, BootstrapError> {
        request.validate()?;
        if token.len() != 64
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(BootstrapError::InvalidJournal);
        }
        let token_hash = digest(token);
        let request_json =
            serde_json::to_string(request).map_err(|_| BootstrapError::InvalidJournal)?;
        self.transaction(|tx| {
            let now = self.now()?;
            let existing: Option<ExistingRegistration> = tx
                .query_row(
                    "SELECT c.principal_id,c.revoked_at IS NULL,
                            c.expires_at IS NULL OR julianday(c.expires_at)>julianday(?),
                            r.request_json,r.response_json
                     FROM credentials c
                     LEFT JOIN registration_receipts r ON r.token_hash=c.token_hash
                     WHERE c.token_hash=?",
                    params![now, token_hash],
                    |row| {
                        Ok(ExistingRegistration {
                            principal_id: row.get(0)?,
                            unrevoked: row.get(1)?,
                            unexpired: row.get(2)?,
                            request_json: row.get(3)?,
                            response_json: row.get(4)?,
                        })
                    },
                )
                .optional()?;
            if let Some(existing) = existing {
                let active: bool = tx.query_row(
                    "SELECT disabled_at IS NULL FROM principals WHERE id=?",
                    [&existing.principal_id],
                    |row| row.get(0),
                )?;
                if !existing.unrevoked || !existing.unexpired || !active {
                    return Err(BootstrapError::Unauthorized);
                }
                let (Some(original), Some(response)) =
                    (existing.request_json, existing.response_json)
                else {
                    return Err(BootstrapError::Unauthorized);
                };
                if original != request_json {
                    return Err(BootstrapError::Conflict);
                }
                let receipt =
                    decode_json(response.as_bytes()).map_err(|_| BootstrapError::CorruptJournal)?;
                return Ok(RegistrationOutcome {
                    receipt,
                    replayed: true,
                });
            }
            self.checkpoint("registration-digest-lookup")?;
            let principal_id = self.principal_id()?;
            tx.execute(
                "INSERT INTO principals(id,display_name,created_at) VALUES (?,?,?)",
                params![principal_id, request.display_name, now],
            )?;
            self.checkpoint("registration-principal")?;
            tx.execute(
                "INSERT INTO principal_names(name,principal_id,kind,created_at)
                 VALUES (?,?,'current',?)",
                params![request.handle, principal_id, now],
            )?;
            self.checkpoint("registration-handle")?;
            let credential_id = format!("cred-{}", self.secret()?);
            tx.execute(
                "INSERT INTO credentials(id,principal_id,class,token_hash,created_at)
                 VALUES (?,?,'principal-client',?,?)",
                params![credential_id, principal_id, token_hash, now],
            )?;
            tx.execute(
                "INSERT INTO credential_audit(credential_id,operation,occurred_at)
                 VALUES (?,'issued',?)",
                params![credential_id, now],
            )?;
            self.checkpoint("registration-credential")?;
            let receipt = RegistrationReceipt {
                principal: principal_descriptor(tx, &principal_id)?,
                credential_id: credential_id.clone(),
            };
            let response_json =
                serde_json::to_string(&receipt).map_err(|_| BootstrapError::CorruptJournal)?;
            tx.execute(
                "INSERT INTO registration_receipts(
                    token_hash,credential_id,principal_id,request_json,response_json,created_at
                 ) VALUES (?,?,?,?,?,?)",
                params![
                    token_hash,
                    credential_id,
                    principal_id,
                    request_json,
                    response_json,
                    now
                ],
            )?;
            self.checkpoint("registration-receipt")?;
            Ok(RegistrationOutcome {
                receipt,
                replayed: false,
            })
        })
    }

    /// Only a principal-client credential can edit its own mutable profile.
    /// A rename atomically retires the current handle into the permanent alias
    /// namespace before publishing the next current handle.
    pub fn update_own_profile(
        &self,
        actor: &AuthenticatedCredential,
        idempotency_key: &str,
        request: &ProfileUpdateRequest,
    ) -> Result<Principal, BootstrapError> {
        request.validate()?;
        if actor.class != CredentialClass::PrincipalClient
            || !(1..=255).contains(&idempotency_key.chars().count())
            || idempotency_key.chars().any(char::is_control)
        {
            return Err(BootstrapError::Unauthorized);
        }
        let payload_hash =
            digest(&serde_json::to_string(request).map_err(|_| BootstrapError::InvalidJournal)?);
        self.transaction(|tx| {
            let now = self.now()?;
            active_principal(tx, &actor.principal_id)?;
            let valid: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM credentials WHERE id=? AND principal_id=?
                 AND class='principal-client' AND revoked_at IS NULL
                 AND (expires_at IS NULL OR julianday(expires_at)>julianday(?)))",
                params![actor.credential_id, actor.principal_id, now],
                |row| row.get(0),
            )?;
            if !valid {
                return Err(BootstrapError::Unauthorized);
            }
            let replay: Option<(String, String)> = tx
                .query_row(
                    "SELECT payload_hash,response_json FROM profile_idempotency_keys
                     WHERE principal_id=? AND idempotency_key=?",
                    params![actor.principal_id, idempotency_key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((previous_hash, response)) = replay {
                if previous_hash != payload_hash {
                    return Err(BootstrapError::IdempotencyConflict);
                }
                return journal_protocol::decode_json(response.as_bytes())
                    .map_err(|_| BootstrapError::CorruptJournal);
            }
            let (current_handle, revision): (String, i64) = tx.query_row(
                "SELECT n.name,p.profile_revision FROM principal_names n
                 JOIN principals p ON p.id=n.principal_id
                 WHERE n.principal_id=? AND n.kind='current'",
                [&actor.principal_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if revision != request.expected_profile_revision {
                return Err(BootstrapError::Conflict);
            }
            if current_handle != request.handle {
                tx.execute(
                    "UPDATE principal_names SET kind='alias'
                     WHERE name=? AND principal_id=? AND kind='current'",
                    params![current_handle, actor.principal_id],
                )?;
                tx.execute(
                    "INSERT INTO principal_names(name,principal_id,kind,created_at)
                     VALUES (?,?,'current',?)",
                    params![request.handle, actor.principal_id, now],
                )?;
            }
            let changed = tx.execute(
                "UPDATE principals SET display_name=?,description=?,profile_revision=profile_revision+1
                 WHERE id=? AND disabled_at IS NULL AND profile_revision=?",
                params![
                    request.display_name,
                    request.description,
                    actor.principal_id,
                    request.expected_profile_revision
                ],
            )?;
            if changed != 1 {
                return Err(BootstrapError::Conflict);
            }
            let principal = principal_descriptor(tx, &actor.principal_id)?;
            tx.execute(
                "INSERT INTO audit_events(id,event_type,actor_principal_id,subject_type,subject_id,detail_json,created_at)
                 VALUES (?,?,?,?,?,?,?)",
                params![
                    format!("profile-{}", self.secret()?),
                    "principal-profile-updated",
                    actor.principal_id,
                    "principal",
                    actor.principal_id,
                    serde_json::json!({
                        "old_handle": current_handle,
                        "new_handle": principal.handle,
                        "previous_profile_revision": revision,
                        "new_profile_revision": principal.profile_revision,
                    })
                    .to_string(),
                    now,
                ],
            )?;
            let response = serde_json::to_string(&principal)
                .map_err(|_| BootstrapError::CorruptJournal)?;
            tx.execute(
                "INSERT INTO profile_idempotency_keys(principal_id,idempotency_key,payload_hash,response_json,created_at)
                 VALUES (?,?,?,?,?)",
                params![actor.principal_id, idempotency_key, payload_hash, response, now],
            )?;
            Ok(principal)
        })
    }

    pub fn create_space(&self, request: &SpaceCreateRequest) -> Result<Space, BootstrapError> {
        request.validate()?;
        let now = self.now()?;
        self.transaction(|tx| {
            tx.execute(
                "INSERT INTO spaces(id,name,access,created_at) VALUES (?,?,?,?)",
                params![request.id, request.name, request.access.as_str(), now],
            )?;
            Ok(())
        })?;
        Ok(Space {
            id: request.id.clone(),
            name: request.name.clone(),
            access: request.access,
            created_at: now,
            archived_at: None,
            limits: default_limits(),
        })
    }

    pub fn set_membership(
        &self,
        request: &MembershipRequest,
    ) -> Result<Membership, BootstrapError> {
        request.validate()?;
        let principal_id = self.transaction(|tx| active_principal(tx, &request.principal_id))?;
        self.transaction(|tx| {
            let space_exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM spaces WHERE id=?)",
                [&request.space_id], |row| row.get(0))?;
            if !space_exists { return Err(BootstrapError::NotFound); }
            tx.execute("INSERT INTO memberships(space_id,principal_id,can_read,can_append,can_admin,created_at)
                VALUES (?,?,?,?,?,?) ON CONFLICT(space_id,principal_id) DO UPDATE SET can_read=excluded.can_read,can_append=excluded.can_append,can_admin=excluded.can_admin",
                params![request.space_id, principal_id, request.can_read, request.can_append, request.can_admin, self.now()?])?;
            Ok(())
        })?;
        Ok(Membership {
            space_id: request.space_id.clone(),
            principal_id,
            can_read: request.can_read,
            can_append: request.can_append,
            can_admin: request.can_admin,
        })
    }

    pub fn authenticate(
        &self,
        secret: &str,
        class: CredentialClass,
    ) -> Result<AuthenticatedCredential, BootstrapError> {
        if secret.len() != 64 {
            return Err(BootstrapError::Unauthorized);
        }
        let connection = self.database.connect_read_only()?;
        connection.query_row(
            "SELECT c.id,c.principal_id FROM credentials c JOIN principals p ON p.id=c.principal_id
             WHERE c.token_hash=? AND c.class=? AND c.revoked_at IS NULL AND p.disabled_at IS NULL
             AND (c.expires_at IS NULL OR julianday(c.expires_at)>julianday(?))",
            params![digest(secret),class_name(class),self.now()?], |r|Ok(AuthenticatedCredential {credential_id:r.get(0)?,principal_id:r.get(1)?,class}))
            .optional()?.ok_or(BootstrapError::Unauthorized)
    }

    /// Verify a principal-client credential for append idempotency without
    /// authorizing a new append. The append transaction performs active-principal
    /// authorization only after checking a durable replay/conflict entry.
    pub fn authenticate_append_replay(
        &self,
        secret: &str,
    ) -> Result<AuthenticatedCredential, BootstrapError> {
        if secret.len() != 64 {
            return Err(BootstrapError::Unauthorized);
        }
        let connection = self.database.connect_read_only()?;
        connection
            .query_row(
                "SELECT id,principal_id FROM credentials
                 WHERE token_hash=? AND class='principal-client' AND revoked_at IS NULL
                 AND (expires_at IS NULL OR julianday(expires_at)>julianday(?))",
                params![digest(secret), self.now()?],
                |row| {
                    Ok(AuthenticatedCredential {
                        credential_id: row.get(0)?,
                        principal_id: row.get(1)?,
                        class: CredentialClass::PrincipalClient,
                    })
                },
            )
            .optional()?
            .ok_or(BootstrapError::Unauthorized)
    }

    pub fn rotate(
        &self,
        request: &CredentialRotateRequest,
    ) -> Result<CredentialRotation, BootstrapError> {
        request.validate()?;
        let now = self.now()?;
        self.transaction(|tx| {
            let row:Option<(String,String,Option<String>)> = tx.query_row("SELECT principal_id,class,expires_at FROM credentials
                WHERE id=? AND revoked_at IS NULL AND (expires_at IS NULL OR julianday(expires_at)>julianday(?))",
                params![request.credential_id,now], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
            let (principal,class,expires_at) = row.ok_or(BootstrapError::NotFound)?;
            active_principal(tx,&principal)?;
            let class=parse_class(&class)?;
            let replacement = self.issue_principal(tx,&principal,&now)?;
            tx.execute("UPDATE credentials SET expires_at=? WHERE id=?", params![expires_at,replacement.credential_id])?;
            self.checkpoint("rotation-issued")?;
            tx.execute("UPDATE credentials SET revoked_at=?,replacement_credential_id=?,revocation_reason=? WHERE id=?",
                params![now,replacement.credential_id,request.reason,request.credential_id])?;
            tx.execute("INSERT INTO credential_audit(credential_id,operation,occurred_at,reason) VALUES (?,'rotated',?,?)",params![request.credential_id,now,request.reason])?;
            self.checkpoint("rotation-revoked")?;
            Ok(CredentialRotation {metadata:CredentialMetadata{credential_id:request.credential_id.clone(),principal_id:principal,class,rotated_at:now.clone(),replacement_credential_id:Some(replacement.credential_id.clone())},replacement_secret:OneTimeReplacementSecret {credential_id:replacement.credential_id,secret:replacement.secret}})
        })
    }

    pub fn revoke(
        &self,
        credential_id: &str,
        reason: Option<&str>,
    ) -> Result<CredentialMetadata, BootstrapError> {
        CredentialRotateRequest {
            credential_id: credential_id.into(),
            reason: reason.map(str::to_owned),
        }
        .validate()?;
        let now = self.now()?;
        self.transaction(|tx| {
            let row:Option<(String,String,Option<String>)> = tx.query_row("SELECT principal_id,class,revoked_at FROM credentials WHERE id=?", [credential_id], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
            let (principal,class,revoked_at) = row.ok_or(BootstrapError::NotFound)?;
            if revoked_at.is_none() {
                tx.execute("UPDATE credentials SET revoked_at=?,revocation_reason=? WHERE id=?", params![now,reason,credential_id])?;
                tx.execute("INSERT INTO credential_audit(credential_id,operation,occurred_at,reason) VALUES (?,'revoked',?,?)",params![credential_id,now,reason])?;
            }
            self.checkpoint("credential-revoked")?;
            Ok(CredentialMetadata {credential_id:credential_id.into(),principal_id:principal,class:parse_class(&class)?,rotated_at:revoked_at.unwrap_or(now.clone()),replacement_credential_id:None})
        })
    }

    pub fn recover_principal(
        &self,
        request: &PrincipalRecoveryRequest,
    ) -> Result<PrincipalRecoveryResponse, BootstrapError> {
        request.validate()?;
        self.transaction(|tx| {
            let now = self.now()?;
            let principal = match principal_descriptor(tx, &request.principal_id) {
                Err(BootstrapError::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                    return Err(BootstrapError::NotFound);
                }
                result => result?,
            };
            tx.execute(
                "INSERT INTO credential_audit(credential_id,operation,occurred_at,reason)
                 SELECT id,'recovered',?,? FROM credentials
                 WHERE principal_id=? AND revoked_at IS NULL
                   AND (expires_at IS NULL OR julianday(expires_at)>julianday(?))",
                params![now, request.reason, request.principal_id, now],
            )?;
            tx.execute(
                "UPDATE credentials
                 SET revoked_at=?,revocation_reason=coalesce(?,'principal recovery')
                 WHERE principal_id=? AND revoked_at IS NULL
                   AND (expires_at IS NULL OR julianday(expires_at)>julianday(?))",
                params![now, request.reason, request.principal_id, now],
            )?;
            self.checkpoint("principal-recovery-revoked")?;
            let replacement = self.issue_principal(tx, &request.principal_id, &now)?;
            self.checkpoint("principal-recovery-issued")?;
            tx.execute(
                "INSERT INTO audit_events(
                    id,event_type,subject_type,subject_id,detail_json,created_at
                 ) VALUES (?,?,?,?,?,?)",
                params![
                    format!("recovery-{}", self.secret()?),
                    "principal-credential-recovered",
                    "principal",
                    request.principal_id,
                    serde_json::json!({"reason": request.reason}).to_string(),
                    now
                ],
            )?;
            Ok(PrincipalRecoveryResponse {
                principal,
                replacement_secret: OneTimeReplacementSecret {
                    credential_id: replacement.credential_id,
                    secret: replacement.secret,
                },
            })
        })
    }

    fn issue_principal(
        &self,
        tx: &Transaction<'_>,
        principal: &str,
        now: &str,
    ) -> Result<OneTimePrincipalClientSecret, BootstrapError> {
        let credential_id = format!("cred-{}", self.secret()?);
        let secret = self.secret()?;
        tx.execute(
            "INSERT INTO credentials(id,principal_id,class,token_hash,created_at)
             VALUES (?,?,'principal-client',?,?)",
            params![credential_id, principal, digest(&secret), now],
        )?;
        tx.execute(
            "INSERT INTO credential_audit(credential_id,operation,occurred_at)
             VALUES (?,'issued',?)",
            params![credential_id, now],
        )?;
        Ok(OneTimePrincipalClientSecret {
            credential_id,
            secret,
        })
    }

    pub fn me(&self, actor: &AuthenticatedCredential) -> Result<Me, BootstrapError> {
        if actor.class != CredentialClass::PrincipalClient {
            return Err(BootstrapError::Unauthorized);
        }
        self.read(|tx| {
            let valid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM credentials WHERE id=? AND principal_id=? AND class='principal-client' AND revoked_at IS NULL AND (expires_at IS NULL OR julianday(expires_at)>julianday(?)))",
                params![actor.credential_id,actor.principal_id,self.now()?],|r|r.get(0))?;
            if !valid {return Err(BootstrapError::Unauthorized);}
            active_principal(tx,&actor.principal_id)?;
            let principal=principal_descriptor(tx,&actor.principal_id)?;
            let mut statement=tx.prepare("SELECT space_id,principal_id,can_read,can_append,can_admin FROM memberships WHERE principal_id=? ORDER BY space_id")?;
            let memberships=statement.query_map([&actor.principal_id],|r|Ok(Membership{space_id:r.get(0)?,principal_id:r.get(1)?,can_read:r.get(2)?,can_append:r.get(3)?,can_admin:r.get(4)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Me {principal,memberships,limits:default_limits()})
        })
    }
}

fn active_principal(tx: &Transaction<'_>, selector: &str) -> Result<String, BootstrapError> {
    // Names take precedence over IDs. This is intentionally a provenance lookup,
    // not a UUID-shaped-string heuristic: a UUID-looking handle remains a handle.
    tx.query_row(
        "SELECT p.id FROM principal_names n JOIN principals p ON p.id=n.principal_id
         WHERE n.name=? AND p.disabled_at IS NULL
         UNION ALL
         SELECT p.id FROM principals p
         WHERE p.id=? AND p.disabled_at IS NULL
           AND NOT EXISTS(SELECT 1 FROM principal_names WHERE name=?)
         LIMIT 1",
        params![selector, selector, selector],
        |row| row.get(0),
    )
    .optional()?
    .ok_or(BootstrapError::NotFound)
}

fn principal_descriptor(
    tx: &Transaction<'_>,
    principal_id: &str,
) -> Result<Principal, BootstrapError> {
    tx.query_row(
        "SELECT p.id,n.name,p.display_name,p.description,p.profile_revision,p.created_at,
                p.disabled_at IS NOT NULL
         FROM principals p JOIN principal_names n ON n.principal_id=p.id AND n.kind='current'
         WHERE p.id=?",
        [principal_id],
        |row| {
            Ok(Principal {
                id: row.get(0)?,
                handle: row.get(1)?,
                display_name: row.get(2)?,
                description: row.get(3)?,
                profile_revision: row.get(4)?,
                created_at: row.get(5)?,
                disabled: row.get(6)?,
            })
        },
    )
    .map_err(Into::into)
}

fn class_name(class: CredentialClass) -> &'static str {
    match class {
        CredentialClass::PrincipalClient => "principal-client",
    }
}
fn parse_class(class: &str) -> Result<CredentialClass, BootstrapError> {
    match class {
        "principal-client" => Ok(CredentialClass::PrincipalClient),
        _ => Err(BootstrapError::Unauthorized),
    }
}
fn timestamp(time: SystemTime) -> Result<String, BootstrapError> {
    let seconds: i64 = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| BootstrapError::Clock)?
        .as_secs()
        .try_into()
        .map_err(|_| BootstrapError::Clock)?;
    Ok(jiff::Timestamp::from_second(seconds)
        .map_err(|_| BootstrapError::Clock)?
        .to_string())
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|b| {
            [
                DIGITS[(b >> 4) as usize] as char,
                DIGITS[(b & 15) as usize] as char,
            ]
        })
        .collect()
}
fn digest(secret: &str) -> String {
    hex(&Sha256::digest(secret.as_bytes()))
}
