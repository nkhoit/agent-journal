//! Host-local provisioning and atomic, one-use enrollment.
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use journal_domain::{Principal, Space, default_limits};
use journal_protocol::*;
use journal_storage_sqlite::{Database, StorageError};
use rusqlite::{OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::Clock;

#[path = "custody.rs"]
mod custody;
#[path = "delivery.rs"]
mod delivery;
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
    pub adapter_id: Option<String>,
    pub instance_id: Option<String>,
}

pub type CredentialRotation = CredentialRotationResponse;

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

    pub fn delivery(
        &self,
        id: &str,
        query: &PageQuery,
    ) -> Result<DeliveryStatusPage, BootstrapError> {
        self.service
            .delivery_status_as(ReadIdentity::ConfiguredViewer(self.principal), id, query)
    }
}

#[derive(Clone)]
pub struct BootstrapService {
    database: Database,
    clock: Arc<dyn Clock + Send + Sync>,
    random: Arc<dyn SecretSource>,
    failpoint: Option<&'static str>,
}

impl BootstrapService {
    pub fn operational_metrics(
        &self,
    ) -> Result<journal_protocol::OperationalMetrics, BootstrapError> {
        let sampled_at = self.now()?;
        let snapshot = self.database.operational_snapshot(&sampled_at)?;
        let recovery = self.database.recovery_status()?;
        Ok(journal_protocol::OperationalMetrics {
            sampled_at,
            database_bytes: snapshot.database_bytes,
            wal_bytes: snapshot.wal_bytes,
            pending_mailbox_count: snapshot.pending_mailbox_count,
            oldest_pending_at: snapshot.oldest_pending_at,
            outstanding_claims: snapshot.outstanding_claims,
            expired_active_claims: snapshot.expired_active_claims,
            expired_claims: snapshot.expired_claims,
            oldest_active_heartbeat_at: snapshot.oldest_active_heartbeat_at,
            stale_registrations_with_pending: snapshot.stale_registrations_with_pending,
            runtime_failure_events: snapshot.runtime_failure_events,
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
                tx.query_row(
                    "SELECT id FROM principals WHERE id=? AND disabled_at IS NULL",
                    [principal],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or(BootstrapError::NotFound)
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
        self.transaction(|tx| {
            tx.execute(
                "INSERT INTO principals(id,display_name,created_at) VALUES (?,?,?)",
                params![request.id, request.display_name, now],
            )?;
            Ok(())
        })?;
        Ok(Principal {
            id: request.id.clone(),
            display_name: Some(request.display_name.clone()),
            created_at: now,
            disabled: false,
        })
    }

    pub fn create_space(&self, request: &SpaceCreateRequest) -> Result<Space, BootstrapError> {
        request.validate()?;
        let now = self.now()?;
        self.transaction(|tx| {
            tx.execute(
                "INSERT INTO spaces(id,name,created_at) VALUES (?,?,?)",
                params![request.id, request.name, now],
            )?;
            Ok(())
        })?;
        Ok(Space {
            id: request.id.clone(),
            name: request.name.clone(),
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
        self.transaction(|tx| {
            active_principal(tx, &request.principal_id)?;
            let space_exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM spaces WHERE id=?)",
                [&request.space_id], |row| row.get(0))?;
            if !space_exists { return Err(BootstrapError::NotFound); }
            tx.execute("INSERT INTO memberships(space_id,principal_id,can_read,can_append,can_admin,created_at)
                VALUES (?,?,?,?,?,?) ON CONFLICT(space_id,principal_id) DO UPDATE SET can_read=excluded.can_read,can_append=excluded.can_append,can_admin=excluded.can_admin",
                params![request.space_id, request.principal_id, request.can_read, request.can_append, request.can_admin, self.now()?])?;
            Ok(())
        })?;
        Ok(Membership {
            space_id: request.space_id.clone(),
            principal_id: request.principal_id.clone(),
            can_read: request.can_read,
            can_append: request.can_append,
            can_admin: request.can_admin,
        })
    }

    pub fn provision_adapter(
        &self,
        request: &AdapterProvisionRequest,
    ) -> Result<AdapterProvisionResponse, BootstrapError> {
        request.validate()?;
        self.transaction(|tx| {
            active_principal(tx, &request.principal_id)?;
            tx.execute(
                "INSERT INTO adapter_identities(adapter_id,principal_id,created_at) VALUES (?,?,?)",
                params![request.adapter_id, request.principal_id, self.now()?],
            )?;
            Ok(())
        })?;
        Ok(AdapterProvisionResponse {
            adapter_id: request.adapter_id.clone(),
            principal_id: request.principal_id.clone(),
        })
    }

    pub fn create_ticket(
        &self,
        request: &EnrollmentTicketCreateRequest,
    ) -> Result<EnrollmentTicketCreateResponse, BootstrapError> {
        request.validate()?;
        let ticket = self.secret()?;
        let now = self.clock.now();
        let expires_at = timestamp(
            now.checked_add(Duration::from_secs(request.ttl_seconds))
                .ok_or(BootstrapError::Clock)?,
        )?;
        self.transaction(|tx| {
            active_principal(tx, &request.principal_id)?;
            let matches: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM adapter_identities WHERE adapter_id=? AND principal_id=?)", params![request.adapter_id, request.principal_id], |r| r.get(0))?;
            if !matches { return Err(BootstrapError::NotFound); }
            let installation: Option<(String, bool)> = tx.query_row("SELECT instance_id,recovery_authorized FROM enrollment_installations WHERE adapter_id=?", [&request.adapter_id], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
            if matches!(&installation, Some((_, false))) { return Err(BootstrapError::Conflict); }
            tx.execute("INSERT INTO enrollment_tickets(ticket_hash,principal_id,adapter_id,expires_at,instance_id) VALUES (?,?,?,?,?)",
                params![digest(&ticket), request.principal_id, request.adapter_id, expires_at, installation.map(|i|i.0)])?;
            Ok(())
        })?;
        Ok(EnrollmentTicketCreateResponse {
            principal_id: request.principal_id.clone(),
            adapter_id: request.adapter_id.clone(),
            expires_at,
            enrollment_ticket: OneTimeEnrollmentTicket { ticket },
        })
    }

    pub fn exchange(
        &self,
        ticket: &str,
        request: &EnrollmentExchangeRequest,
    ) -> Result<EnrollmentExchangeResponse, BootstrapError> {
        request.validate()?;
        if ticket.len() != 64 {
            return Err(BootstrapError::Unauthorized);
        }
        self.transaction(|tx| {
            let instant = self.clock.now();
            let now = timestamp(instant)?;
            let lease = timestamp(instant.checked_add(Duration::from_secs(60)).ok_or(BootstrapError::Clock)?)?;
            let binding: Option<(String,String,Option<String>)> = tx.query_row(
                "SELECT principal_id,adapter_id,instance_id FROM enrollment_tickets WHERE ticket_hash=? AND consumed_at IS NULL AND invalidated_at IS NULL AND julianday(expires_at)>julianday(?)",
                params![digest(ticket),now], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
            let (principal,adapter,bound_instance) = binding.ok_or(BootstrapError::Unauthorized)?;
            self.checkpoint("ticket-lookup")?;
            active_principal(tx, &principal).map_err(|error| match error {
                BootstrapError::NotFound => BootstrapError::Unauthorized,
                other => other,
            })?;
            if bound_instance.as_ref().is_some_and(|id| id != &request.instance_id) { return Err(BootstrapError::Conflict); }
            let installation: Option<(String,bool)> = tx.query_row("SELECT instance_id,recovery_authorized FROM enrollment_installations WHERE adapter_id=?", [&adapter], |r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            if let Some((instance,recovery)) = &installation {
                if instance != &request.instance_id || !recovery { return Err(BootstrapError::Conflict); }
            }
            let client = self.issue(tx, &principal, CredentialClass::PrincipalClient, &adapter, &request.instance_id, &now)?;
            self.checkpoint("principal-credential")?;
            let delivery = self.issue(tx, &principal, CredentialClass::DeliveryAdapter, &adapter, &request.instance_id, &now)?;
            self.checkpoint("delivery-credential")?;
            tx.execute("INSERT INTO enrollment_installations(adapter_id,instance_id,created_at) VALUES (?,?,?)
                ON CONFLICT(adapter_id) DO UPDATE SET recovery_authorized=0", params![adapter,request.instance_id,now])?;
            tx.execute("INSERT INTO adapter_registrations(adapter_id,principal_id,instance_id,generation,status,last_heartbeat_at,lease_expires_at,created_at)
                VALUES (?,?,?,1,'active',?,?,?) ON CONFLICT(adapter_id) DO UPDATE SET
                generation=generation+1,status='active',last_heartbeat_at=excluded.last_heartbeat_at,lease_expires_at=excluded.lease_expires_at",
                params![adapter,principal,request.instance_id,now,lease,now])?;
            self.checkpoint("registration")?;
            tx.execute("UPDATE enrollment_tickets SET consumed_at=? WHERE ticket_hash=? AND consumed_at IS NULL", params![now,digest(ticket)])?;
            tx.execute("UPDATE enrollment_tickets SET invalidated_at=? WHERE adapter_id=? AND consumed_at IS NULL AND invalidated_at IS NULL",params![now,adapter])?;
            self.checkpoint("ticket-consumed")?;
            let generation = tx.query_row("SELECT generation FROM adapter_registrations WHERE adapter_id=?", [&adapter], |r|r.get(0))?;
            Ok(EnrollmentExchangeResponse { adapter_id:adapter, principal_id:principal, instance_id:request.instance_id.clone(), generation,
                principal_client_secret:client, delivery_adapter_secret:OneTimeDeliveryAdapterSecret {credential_id:delivery.credential_id,secret:delivery.secret} })
        })
    }

    fn issue(
        &self,
        tx: &Transaction<'_>,
        principal: &str,
        class: CredentialClass,
        adapter: &str,
        instance: &str,
        now: &str,
    ) -> Result<OneTimePrincipalClientSecret, BootstrapError> {
        let credential_id = format!("cred-{}", self.secret()?);
        let secret = self.secret()?;
        let delivery_adapter = (class == CredentialClass::DeliveryAdapter).then_some(adapter);
        tx.execute("INSERT INTO credentials(id,principal_id,class,token_hash,adapter_id,created_at,enrollment_adapter_id,instance_id) VALUES (?,?,?,?,?,?,?,?)",
            params![credential_id,principal,class_name(class),digest(&secret),delivery_adapter,now,adapter,instance])?;
        tx.execute("INSERT INTO credential_audit(credential_id,operation,occurred_at) VALUES (?,'issued',?)", params![credential_id,now])?;
        Ok(OneTimePrincipalClientSecret {
            credential_id,
            secret,
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
            "SELECT c.id,c.principal_id,c.adapter_id,c.instance_id FROM credentials c JOIN principals p ON p.id=c.principal_id
             WHERE c.token_hash=? AND c.class=? AND c.revoked_at IS NULL AND p.disabled_at IS NULL
             AND (c.expires_at IS NULL OR julianday(c.expires_at)>julianday(?))
             AND (c.class='principal-client' OR EXISTS(SELECT 1 FROM adapter_registrations r WHERE r.adapter_id=c.adapter_id AND r.instance_id=c.instance_id AND r.status='active'))",
            params![digest(secret),class_name(class),self.now()?], |r|Ok(AuthenticatedCredential {credential_id:r.get(0)?,principal_id:r.get(1)?,class,adapter_id:r.get(2)?,instance_id:r.get(3)?}))
            .optional()?.ok_or(BootstrapError::Unauthorized)
    }

    pub fn rotate(
        &self,
        request: &CredentialRotateRequest,
    ) -> Result<CredentialRotation, BootstrapError> {
        request.validate()?;
        let now = self.now()?;
        self.transaction(|tx| {
            let row:Option<(String,String,String,String,Option<String>)> = tx.query_row("SELECT principal_id,class,enrollment_adapter_id,instance_id,expires_at FROM credentials
                WHERE id=? AND revoked_at IS NULL AND (expires_at IS NULL OR julianday(expires_at)>julianday(?))",
                params![request.credential_id,now], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
            let (principal,class,adapter,instance,expires_at) = row.ok_or(BootstrapError::NotFound)?;
            active_principal(tx,&principal)?;
            let class=parse_class(&class)?;
            let replacement = self.issue(tx,&principal,class,&adapter,&instance,&now)?;
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

    pub fn recover(
        &self,
        adapter_id: &str,
        instance_id: &str,
    ) -> Result<AdapterProvisionResponse, BootstrapError> {
        AdapterPath {
            adapter_id: adapter_id.into(),
        }
        .validate()?;
        EnrollmentExchangeRequest {
            instance_id: instance_id.into(),
        }
        .validate()?;
        let now = self.now()?;
        self.transaction(|tx| {
            let principal:Option<String> = tx.query_row("SELECT a.principal_id FROM adapter_identities a JOIN enrollment_installations i ON i.adapter_id=a.adapter_id WHERE a.adapter_id=? AND i.instance_id=?",params![adapter_id,instance_id],|r|r.get(0)).optional()?;
            let principal=principal.ok_or(BootstrapError::NotFound)?;
            // Recovery revokes the whole enrollment lineage, including rotated replacements.
            tx.execute("INSERT INTO credential_audit(credential_id,operation,occurred_at) SELECT id,'recovered',? FROM credentials WHERE enrollment_adapter_id=? AND instance_id=? AND revoked_at IS NULL",params![now,adapter_id,instance_id])?;
            tx.execute("UPDATE credentials SET revoked_at=?,revocation_reason='enrollment recovery' WHERE enrollment_adapter_id=? AND instance_id=? AND revoked_at IS NULL",params![now,adapter_id,instance_id])?;
            self.checkpoint("recovery-revoked")?;
            delivery::close_claims(tx, adapter_id, &now, true)?;
            tx.execute("UPDATE adapter_registrations SET status='revoked' WHERE adapter_id=? AND instance_id=?",params![adapter_id,instance_id])?;
            tx.execute("UPDATE enrollment_tickets SET invalidated_at=? WHERE adapter_id=? AND consumed_at IS NULL AND invalidated_at IS NULL",params![now,adapter_id])?;
            tx.execute("UPDATE enrollment_installations SET recovery_authorized=1 WHERE adapter_id=? AND instance_id=?",params![adapter_id,instance_id])?;
            self.checkpoint("recovery-authorized")?;
            Ok(AdapterProvisionResponse {adapter_id:adapter_id.into(),principal_id:principal})
        })
    }

    pub fn me(&self, actor: &AuthenticatedCredential) -> Result<Me, BootstrapError> {
        if actor.class != CredentialClass::PrincipalClient {
            return Err(BootstrapError::Unauthorized);
        }
        self.transaction(|tx| {
            let valid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM credentials WHERE id=? AND principal_id=? AND class='principal-client' AND revoked_at IS NULL AND (expires_at IS NULL OR julianday(expires_at)>julianday(?)))",
                params![actor.credential_id,actor.principal_id,self.now()?],|r|r.get(0))?;
            if !valid {return Err(BootstrapError::Unauthorized);}
            active_principal(tx,&actor.principal_id)?;
            let principal=tx.query_row("SELECT id,display_name,created_at FROM principals WHERE id=?",[&actor.principal_id],|r|Ok(Principal{id:r.get(0)?,display_name:Some(r.get(1)?),created_at:r.get(2)?,disabled:false}))?;
            let mut statement=tx.prepare("SELECT space_id,principal_id,can_read,can_append,can_admin FROM memberships WHERE principal_id=? ORDER BY space_id")?;
            let memberships=statement.query_map([&actor.principal_id],|r|Ok(Membership{space_id:r.get(0)?,principal_id:r.get(1)?,can_read:r.get(2)?,can_append:r.get(3)?,can_admin:r.get(4)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Me {principal,memberships,limits:default_limits()})
        })
    }
}

fn active_principal(tx: &Transaction<'_>, principal: &str) -> Result<(), BootstrapError> {
    let active: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM principals WHERE id=? AND disabled_at IS NULL)",
        [principal],
        |r| r.get(0),
    )?;
    if active {
        Ok(())
    } else {
        Err(BootstrapError::NotFound)
    }
}
fn class_name(class: CredentialClass) -> &'static str {
    match class {
        CredentialClass::PrincipalClient => "principal-client",
        CredentialClass::DeliveryAdapter => "delivery-adapter",
    }
}
fn parse_class(class: &str) -> Result<CredentialClass, BootstrapError> {
    match class {
        "principal-client" => Ok(CredentialClass::PrincipalClient),
        "delivery-adapter" => Ok(CredentialClass::DeliveryAdapter),
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
