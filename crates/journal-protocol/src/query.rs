use crate::{ListPrincipalsQuery, ListRecordsQuery, PageQuery};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidQuery;

pub fn path_segment(value: &str) -> String {
    form_urlencoded::byte_serialize(value.as_bytes())
        .collect::<String>()
        .replace('+', "%20")
}

pub fn query_string(values: &[(String, String)]) -> String {
    form_urlencoded::Serializer::new(String::new())
        .extend_pairs(values)
        .finish()
}

fn parse(query: &str, allowed: &[&str]) -> Result<BTreeMap<String, String>, InvalidQuery> {
    // Reject malformed percent escapes and invalid UTF-8 rather than replacing
    // bytes and silently changing a filter's meaning.
    let bytes = query.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'%'
            && (i + 2 >= bytes.len()
                || !bytes[i + 1].is_ascii_hexdigit()
                || !bytes[i + 2].is_ascii_hexdigit())
        {
            return Err(InvalidQuery);
        }
    }
    percent_encoding::percent_decode_str(query)
        .decode_utf8()
        .map_err(|_| InvalidQuery)?;
    let mut result = BTreeMap::new();
    for (key, value) in form_urlencoded::parse(bytes) {
        if !allowed.contains(&key.as_ref())
            || result
                .insert(key.into_owned(), value.into_owned())
                .is_some()
        {
            return Err(InvalidQuery);
        }
    }
    Ok(result)
}

fn page(values: &BTreeMap<String, String>) -> Result<PageQuery, InvalidQuery> {
    let page = PageQuery {
        cursor: values.get("cursor").cloned(),
        limit: values
            .get("limit")
            .map(|s| s.parse())
            .transpose()
            .map_err(|_| InvalidQuery)?,
    };
    page.validate().map_err(|_| InvalidQuery)?;
    Ok(page)
}

impl PageQuery {
    pub fn from_query(query: &str) -> Result<Self, InvalidQuery> {
        page(&parse(query, &["cursor", "limit"])?)
    }
    pub fn pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        if let Some(cursor) = &self.cursor {
            pairs.push(("cursor".into(), cursor.clone()));
        }
        if let Some(limit) = self.limit {
            pairs.push(("limit".into(), limit.to_string()));
        }
        pairs
    }
}
impl ListPrincipalsQuery {
    pub fn from_query(query: &str) -> Result<Self, InvalidQuery> {
        let values = parse(query, &["space", "cursor", "limit"])?;
        let result = Self {
            space: values.get("space").cloned().ok_or(InvalidQuery)?,
            page: page(&values)?,
        };
        result.validate().map_err(|_| InvalidQuery)?;
        Ok(result)
    }
    pub fn pairs(&self) -> Vec<(String, String)> {
        let mut pairs = self.page.pairs();
        pairs.push(("space".into(), self.space.clone()));
        pairs
    }
}
impl ListRecordsQuery {
    pub fn from_query(query: &str) -> Result<Self, InvalidQuery> {
        let values = parse(
            query,
            &[
                "cursor",
                "limit",
                "after_seq",
                "author",
                "attention",
                "kind",
                "relation",
            ],
        )?;
        let relation = values
            .get("relation")
            .map(|s| {
                crate::domain::RELATION_TYPES
                    .into_iter()
                    .find(|r| r.as_str() == s)
                    .ok_or(InvalidQuery)
            })
            .transpose()?;
        let result = Self {
            page: page(&values)?,
            after_seq: values
                .get("after_seq")
                .map(|s| s.parse())
                .transpose()
                .map_err(|_| InvalidQuery)?,
            author: values.get("author").cloned(),
            attention: values.get("attention").cloned(),
            kind: values.get("kind").cloned(),
            relation,
        };
        result.validate().map_err(|_| InvalidQuery)?;
        Ok(result)
    }
    pub fn pairs(&self) -> Vec<(String, String)> {
        let mut pairs = self.page.pairs();
        if let Some(seq) = self.after_seq {
            pairs.push(("after_seq".into(), seq.to_string()));
        }
        for (key, value) in [
            ("author", &self.author),
            ("attention", &self.attention),
            ("kind", &self.kind),
        ] {
            if let Some(value) = value {
                pairs.push((key.into(), value.clone()));
            }
        }
        if let Some(relation) = self.relation {
            pairs.push(("relation".into(), relation.to_string()));
        }
        pairs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_roundtrip_and_rejections() {
        let query = ListRecordsQuery {
            kind: Some("note / ? + 界".into()),
            ..Default::default()
        };
        assert_eq!(
            ListRecordsQuery::from_query(&query_string(&query.pairs())).unwrap(),
            query
        );
        assert_eq!(path_segment("a b/c?"), "a%20b%2Fc%3F");
        for value in [
            "limit=0",
            "limit=101",
            "limit=1&limit=2",
            "author=%xx",
            "author=%ff",
            "unknown=x",
            "relation=bogus",
        ] {
            assert!(ListRecordsQuery::from_query(value).is_err(), "{value}");
        }
    }
}
