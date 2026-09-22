//! Runtime-neutral journal domain types, limits, and validation.

use serde::{Deserialize, Deserializer, Serialize, de, de::Visitor};
use thiserror::Error;

pub const MAX_CONTENT_BYTES: usize = 64 * 1024;
pub const MAX_RELATIONS: usize = 32;
pub const MAX_ATTENTION_RECIPIENTS: usize = 16;
pub const MAX_PAGE_SIZE: usize = 100;
pub const MAX_IDENTIFIER_CHARS: usize = 128;
/// Maximum title length in Unicode scalar values, enforced after trimming.
pub const MAX_TITLE_CHARS: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub id: String,
    pub handle: String,
    pub display_name: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub description: Option<String>,
    pub profile_revision: i64,
    pub created_at: String,
    pub disabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpaceAccess {
    Public,
}

impl SpaceAccess {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Space {
    pub id: String,
    pub name: String,
    pub access: SpaceAccess,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<String>,
    pub created_at: String,
    pub limits: Limits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RelationType {
    ReplyTo,
    Supersedes,
    Tombstones,
    RefersTo,
    Acknowledges,
}

impl RelationType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReplyTo => "reply-to",
            Self::Supersedes => "supersedes",
            Self::Tombstones => "tombstones",
            Self::RefersTo => "refers-to",
            Self::Acknowledges => "acknowledges",
        }
    }
}

impl std::fmt::Display for RelationType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub const RELATION_TYPES: [RelationType; 5] = [
    RelationType::ReplyTo,
    RelationType::Supersedes,
    RelationType::Tombstones,
    RelationType::RefersTo,
    RelationType::Acknowledges,
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Relation {
    #[serde(rename = "type")]
    pub relation_type: RelationType,
    pub record_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordInput {
    pub kind: String,
    pub content: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attention: Vec<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub routing_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<Relation>,
    /// Optional human-readable title. Allowed on any record; the thread's
    /// display title is resolved from the root record's title.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub title: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub space_id: String,
    pub seq: i64,
    pub author: String,
    pub kind: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attention: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
    pub relations: Vec<Relation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResult {
    pub record: Record,
    pub mailbox_created: usize,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub content_bytes: usize,
    pub relations: usize,
    pub attention_recipients: usize,
    pub page_size: usize,
}

pub fn default_limits() -> Limits {
    Limits {
        content_bytes: MAX_CONTENT_BYTES,
        relations: MAX_RELATIONS,
        attention_recipients: MAX_ATTENTION_RECIPIENTS,
        page_size: MAX_PAGE_SIZE,
    }
}

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("{field}: must be 1..128 characters")]
    InvalidIdentifier { field: String },
    #[error("content: must not be empty")]
    EmptyContent,
    #[error("content: exceeds {max} bytes")]
    ContentTooLarge { max: usize },
    #[error("relations: exceeds {max} items")]
    TooManyRelations { max: usize },
    #[error("attention: exceeds {max} recipients")]
    TooManyAttention { max: usize },
    #[error("attention: duplicate principal {principal:?}")]
    DuplicateAttention { principal: String },
    #[error("relations: at most one {relation_type:?} relation is allowed")]
    MultipleReplyTo { relation_type: RelationType },
    #[error("limit: must be between 1 and {max}")]
    InvalidPageSize { max: usize },
    #[error("title: must be 1..=200 characters after trimming")]
    InvalidTitle,
    #[error("title: must not contain control characters")]
    TitleControlCharacters,
}

/// Normalize a submitted title: trim Unicode whitespace; empty becomes `None`.
/// Length and control-character validation apply to the trimmed value.
pub fn normalize_title(raw: Option<String>) -> Option<String> {
    raw.and_then(|title| {
        let trimmed = title.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

pub fn validate_title(title: &str) -> Result<(), ValidationError> {
    if title.chars().count() > MAX_TITLE_CHARS {
        return Err(ValidationError::InvalidTitle);
    }
    if title.chars().any(|c| c.is_control()) {
        return Err(ValidationError::TitleControlCharacters);
    }
    Ok(())
}

impl RecordInput {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_identifier("kind", &self.kind)?;
        if self.content.is_empty() {
            return Err(ValidationError::EmptyContent);
        }
        if self.content.len() > MAX_CONTENT_BYTES {
            return Err(ValidationError::ContentTooLarge {
                max: MAX_CONTENT_BYTES,
            });
        }
        if self.relations.len() > MAX_RELATIONS {
            return Err(ValidationError::TooManyRelations { max: MAX_RELATIONS });
        }
        if self.attention.len() > MAX_ATTENTION_RECIPIENTS {
            return Err(ValidationError::TooManyAttention {
                max: MAX_ATTENTION_RECIPIENTS,
            });
        }

        let mut seen = std::collections::BTreeSet::new();
        for principal in &self.attention {
            validate_identifier("attention principal", principal)?;
            if !seen.insert(principal) {
                return Err(ValidationError::DuplicateAttention {
                    principal: principal.clone(),
                });
            }
        }

        if let Some(run_id) = self.run_id.as_deref().filter(|id| !id.is_empty()) {
            validate_identifier("run_id", run_id)?;
        }

        let mut reply_to_count = 0;
        for relation in &self.relations {
            validate_identifier("relation record", &relation.record_id)?;
            if relation.relation_type == RelationType::ReplyTo {
                reply_to_count += 1;
                if reply_to_count > 1 {
                    return Err(ValidationError::MultipleReplyTo {
                        relation_type: RelationType::ReplyTo,
                    });
                }
            }
        }
        if let Some(routing_key) = self.routing_key.as_deref().filter(|key| !key.is_empty()) {
            validate_identifier("routing_key", routing_key)?;
        }
        if let Some(title) = normalize_title(self.title.clone()) {
            validate_title(&title)?;
        }
        Ok(())
    }
}

pub fn validate_page_size(size: usize) -> Result<(), ValidationError> {
    if !(1..=MAX_PAGE_SIZE).contains(&size) {
        return Err(ValidationError::InvalidPageSize { max: MAX_PAGE_SIZE });
    }
    Ok(())
}

pub fn validate_identifier(field: &str, value: &str) -> Result<(), ValidationError> {
    if value.is_empty() || value.chars().count() > MAX_IDENTIFIER_CHARS {
        return Err(ValidationError::InvalidIdentifier {
            field: field.to_owned(),
        });
    }
    Ok(())
}

/// An optional OpenAPI string is either absent or a non-null string. `Option`
/// alone would also accept an explicit JSON `null`, which is not equivalent to
/// an omitted optional string in the request schemas.
fn deserialize_optional_non_null_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptionalNonNullString;

    impl<'de> Visitor<'de> for OptionalNonNullString {
        type Value = Option<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a non-null string")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("null is not allowed"))
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            String::deserialize(deserializer).map(Some)
        }
    }

    deserializer.deserialize_option(OptionalNonNullString)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_input() -> RecordInput {
        RecordInput {
            kind: "message".into(),
            content: "safe".into(),
            run_id: None,
            attention: vec!["beta".into()],
            routing_key: None,
            relations: vec![Relation {
                relation_type: RelationType::ReplyTo,
                record_id: "record-1".into(),
            }],
            title: None,
        }
    }

    #[test]
    fn validates_limits_and_duplicates() {
        assert!(valid_input().validate().is_ok());
        let mut too_large = valid_input();
        too_large.content = "x".repeat(MAX_CONTENT_BYTES + 1);
        assert!(too_large.validate().is_err());
        let mut duplicate = valid_input();
        duplicate.attention.push("beta".into());
        assert!(duplicate.validate().is_err());
        let mut exact_attention = valid_input();
        exact_attention.attention = (0..MAX_ATTENTION_RECIPIENTS)
            .map(|index| format!("agent-{index}"))
            .collect();
        assert!(exact_attention.validate().is_ok());
        let mut too_many_attention = exact_attention;
        too_many_attention
            .attention
            .push(format!("agent-{MAX_ATTENTION_RECIPIENTS}"));
        assert!(too_many_attention.validate().is_err());

        let mut exact_relations = valid_input();
        exact_relations.relations = (0..MAX_RELATIONS)
            .map(|index| Relation {
                relation_type: RelationType::RefersTo,
                record_id: format!("record-{index}"),
            })
            .collect();
        assert!(exact_relations.validate().is_ok());
        let mut too_many_relations = exact_relations;
        too_many_relations.relations.push(Relation {
            relation_type: RelationType::RefersTo,
            record_id: format!("record-{MAX_RELATIONS}"),
        });
        assert!(too_many_relations.validate().is_err());
    }

    #[test]
    fn counts_content_utf8_bytes() {
        let exact = RecordInput {
            kind: "message".into(),
            content: "界".repeat(MAX_CONTENT_BYTES / 3) + "a",
            ..valid_input()
        };
        assert_eq!(exact.content.len(), MAX_CONTENT_BYTES);
        assert!(exact.validate().is_ok());
        let mut over = exact;
        over.content.push('a');
        assert!(over.validate().is_err());
    }

    #[test]
    fn rejects_empty_content() {
        let empty = RecordInput {
            kind: "message".into(),
            content: String::new(),
            ..valid_input()
        };
        assert!(empty.validate().is_err());
    }

    #[test]
    fn accepts_exact_relation_vocabulary() {
        for relation_type in RELATION_TYPES {
            let input = RecordInput {
                kind: "message".into(),
                content: "x".into(),
                relations: vec![Relation {
                    relation_type,
                    record_id: "record-1".into(),
                }],
                ..valid_input()
            };
            assert!(input.validate().is_ok(), "{relation_type}");
        }
    }

    #[test]
    fn rejects_multiple_reply_to_and_unknown_relations() {
        let mut input = valid_input();
        input.relations.push(Relation {
            relation_type: RelationType::ReplyTo,
            record_id: "record-2".into(),
        });
        assert!(input.validate().is_err());
        assert!(
            serde_json::from_str::<Relation>(r#"{"type":"executes","record_id":"record-1"}"#,)
                .is_err()
        );
    }

    #[test]
    fn validates_page_size_boundaries() {
        assert!(validate_page_size(0).is_err());
        assert!(validate_page_size(MAX_PAGE_SIZE + 1).is_err());
        assert!(validate_page_size(MAX_PAGE_SIZE).is_ok());
    }

    #[test]
    fn optional_request_strings_distinguish_absence_from_null() {
        let omitted: RecordInput = serde_json::from_str(r#"{"kind":"message","content":"x"}"#)
            .expect("omitted optional strings");
        assert_eq!(omitted.run_id, None);
        assert_eq!(omitted.routing_key, None);

        assert!(
            serde_json::from_str::<RecordInput>(
                r#"{"kind":"message","content":"x","run_id":null}"#,
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<RecordInput>(
                r#"{"kind":"message","content":"x","routing_key":null}"#,
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<RecordInput>(
                r#"{"kind":"message","content":"x","unexpected":true}"#,
            )
            .is_err()
        );
    }

    #[test]
    fn record_wire_shape_accepts_nullable_fields_and_requires_relations() {
        let record = Record {
            id: "record-1".into(),
            space_id: "space".into(),
            seq: 1,
            author: "alpha".into(),
            kind: "message".into(),
            content: "body".into(),
            run_id: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            attention: Vec::new(),
            routing_key: None,
            relations: Vec::new(),
            title: None,
        };
        let serialized = serde_json::to_value(&record).expect("record JSON");
        assert_eq!(serialized["run_id"], serde_json::Value::Null);
        assert_eq!(serialized["routing_key"], serde_json::Value::Null);
        assert_eq!(serialized["relations"], serde_json::json!([]));

        let with_nulls = serde_json::to_string(&serialized).expect("record JSON text");
        let parsed = serde_json::from_str::<Record>(&with_nulls);
        assert!(parsed.is_ok(), "record JSON did not round-trip: {parsed:?}");
        let without_relations = serialized
            .as_object()
            .expect("record object")
            .iter()
            .filter(|(key, _)| key.as_str() != "relations")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<serde_json::Map<_, _>>();
        assert!(
            serde_json::from_value::<Record>(serde_json::Value::Object(without_relations)).is_err()
        );
    }

    #[test]
    fn normalize_title_trims_and_blanks_to_none() {
        assert_eq!(normalize_title(None), None);
        assert_eq!(normalize_title(Some("".into())), None);
        assert_eq!(normalize_title(Some("   ".into())), None);
        assert_eq!(normalize_title(Some("\u{2003}\t\n".into())), None);
        assert_eq!(
            normalize_title(Some("  hello world  ".into())),
            Some("hello world".into())
        );
        // Interior whitespace is preserved.
        assert_eq!(normalize_title(Some("a  b".into())), Some("a  b".into()));
    }

    #[test]
    fn validate_title_enforces_scalar_limit_and_rejects_controls() {
        // Exactly 200 Unicode scalar values is allowed.
        let ok = "界".repeat(200);
        assert_eq!(ok.chars().count(), 200);
        assert!(validate_title(&ok).is_ok());

        // 201 scalars is rejected even though byte length differs.
        let over = "界".repeat(201);
        assert_eq!(over.chars().count(), 201);
        assert!(matches!(
            validate_title(&over),
            Err(ValidationError::InvalidTitle)
        ));

        // Control characters are rejected.
        assert!(matches!(
            validate_title("hello\x00world"),
            Err(ValidationError::TitleControlCharacters)
        ));
        assert!(matches!(
            validate_title("a\tb"),
            Err(ValidationError::TitleControlCharacters)
        ));

        // Non-control Unicode (including bidi marks) is allowed.
        assert!(validate_title("hello \u{202E}world").is_ok());
    }

    #[test]
    fn record_input_validates_normalized_title() {
        let mut input = valid_input();
        input.title = Some("  Project kickoff  ".into());
        assert!(input.validate().is_ok());

        // Blank titles normalize away and validate fine.
        input.title = Some("   ".into());
        assert!(input.validate().is_ok());

        // Overlong titles fail validation.
        input.title = Some("x".repeat(201));
        assert!(input.validate().is_err());

        // Control characters fail validation.
        input.title = Some("bad\x07title".into());
        assert!(input.validate().is_err());
    }
}
