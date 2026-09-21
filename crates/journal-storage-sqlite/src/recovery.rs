use rusqlite::{Connection, types::ValueRef};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Database, StorageError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableVerification {
    pub name: String,
    pub rows: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryVerification {
    pub tables: Vec<TableVerification>,
    pub space_heads: Vec<(String, i64)>,
}

impl Database {
    /// Runs all probes against one snapshot, not independently changing connections.
    pub fn recovery_verification(&self) -> Result<RecoveryVerification, StorageError> {
        let _guard = self
            .recovery_audit()
            .map(|audit| audit.lock())
            .transpose()?;
        if let Some(audit) = self.recovery_audit() {
            audit.ensure_open(self)?;
        }
        self.recovery_verification_unguarded()
    }

    // Offline reconciliation must probe while the recovery gate is closed.
    pub(crate) fn recovery_verification_unguarded(
        &self,
    ) -> Result<RecoveryVerification, StorageError> {
        let mut connection = self.connect_unchecked()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO records_fts(records_fts) VALUES('integrity-check')",
            [],
        )?;
        verify(&transaction)
    }
}

pub(crate) fn verify(connection: &Connection) -> Result<RecoveryVerification, StorageError> {
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(StorageError::RecoveryClosed(
            "SQLite integrity probe failed",
        ));
    }
    let foreign_keys: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
        [],
        |row| row.get(0),
    )?;
    if foreign_keys {
        return Err(StorageError::RecoveryClosed("foreign key probe failed"));
    }
    for (probe, sql) in [
        ("FTS content probe failed",
         "SELECT EXISTS(SELECT id,space_id,content,author_principal_id,kind,run_id FROM records
          EXCEPT SELECT record_id,space_id,content,author_principal_id,kind,run_id FROM records_fts)
          OR EXISTS(SELECT record_id,space_id,content,author_principal_id,kind,run_id FROM records_fts
          EXCEPT SELECT id,space_id,content,author_principal_id,kind,run_id FROM records)
          OR (SELECT count(*) FROM records)!=(SELECT count(*) FROM records_fts)"),
        ("sequence probe failed",
         "SELECT EXISTS(SELECT space_id FROM records GROUP BY space_id
          HAVING min(space_seq)!=1 OR max(space_seq)!=count(*))"),
        ("mailbox attention probe failed",
         "SELECT EXISTS(SELECT record_id,recipient_principal_id FROM attention
          EXCEPT SELECT record_id,recipient_principal_id FROM mailbox_items)
          OR EXISTS(SELECT record_id,recipient_principal_id FROM mailbox_items
          EXCEPT SELECT record_id,recipient_principal_id FROM attention)"),
        ("inbox allocation probe failed",
         "SELECT EXISTS(SELECT 1 FROM mailbox_items m LEFT JOIN inbox_sequences h
          ON h.recipient_principal_id=m.recipient_principal_id
          WHERE h.last_seq IS NULL OR h.last_seq<m.recipient_seq OR m.recipient_seq<=0)
          OR EXISTS(SELECT 1 FROM inbox_sequences WHERE typeof(last_seq)!='integer' OR last_seq<=0)"),
        ("mailbox attempt probe failed",
         "SELECT EXISTS(SELECT 1 FROM mailbox_items m WHERE NOT EXISTS(
          SELECT 1 FROM delivery_attempts a WHERE a.mailbox_item_id=m.id))
          OR EXISTS(SELECT mailbox_item_id FROM delivery_attempts GROUP BY mailbox_item_id
          HAVING min(ordinal)!=1 OR max(ordinal)!=count(*))
          OR EXISTS(SELECT 1 FROM mailbox_items m JOIN delivery_attempts a ON a.mailbox_item_id=m.id
          WHERE a.ordinal=(SELECT max(ordinal) FROM delivery_attempts WHERE mailbox_item_id=m.id)
          AND m.state!=a.state)"),
    ] {
        if connection.query_row(sql, [], |row| row.get::<_, bool>(0))? {
            return Err(StorageError::RecoveryClosed(probe));
        }
    }
    // Exercise the query path separately from FTS's full internal integrity command.
    connection.query_row(
        "SELECT count(*) FROM records_fts WHERE records_fts MATCH 'agentjournalverificationtoken'",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let mut tables = Vec::new();
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'
         AND name NOT LIKE 'records_fts%' ORDER BY name",
    )?;
    for name in statement.query_map([], |row| row.get::<_, String>(0))? {
        let name = name?;
        tables.push(hash_table(connection, &name)?);
    }
    let mut heads = connection.prepare(
        "SELECT s.id,coalesce(max(r.space_seq),0) FROM spaces s
         LEFT JOIN records r ON r.space_id=s.id GROUP BY s.id ORDER BY s.id",
    )?;
    let space_heads = heads
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecoveryVerification {
        tables,
        space_heads,
    })
}

fn hash_table(connection: &Connection, name: &str) -> Result<TableVerification, StorageError> {
    let quoted = name.replace('"', "\"\"");
    let column_count = connection
        .prepare(&format!("SELECT * FROM \"{quoted}\""))?
        .column_count();
    let order = (1..=column_count)
        .map(|index| index.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let mut statement =
        connection.prepare(&format!("SELECT * FROM \"{quoted}\" ORDER BY {order}"))?;
    let mut rows = statement.query([])?;
    let mut hash = Sha256::new();
    let mut count = 0;
    while let Some(row) = rows.next()? {
        hash.update([0xff]);
        for index in 0..column_count {
            match row.get_ref(index)? {
                ValueRef::Null => hash.update([0]),
                ValueRef::Integer(value) => {
                    hash.update([1]);
                    hash.update(value.to_be_bytes());
                }
                ValueRef::Real(value) => {
                    hash.update([2]);
                    hash.update(value.to_bits().to_be_bytes());
                }
                ValueRef::Text(value) | ValueRef::Blob(value) => {
                    hash.update([if matches!(row.get_ref(index)?, ValueRef::Text(_)) {
                        3
                    } else {
                        4
                    }]);
                    hash.update((value.len() as u64).to_be_bytes());
                    hash.update(value);
                }
            }
        }
        count += 1;
    }
    Ok(TableVerification {
        name: name.to_owned(),
        rows: count,
        sha256: format!("{:x}", hash.finalize()),
    })
}
