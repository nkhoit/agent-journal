use crate::{ListPrincipalsQuery, ListRecordsQuery, PageQuery, SearchOrder, SearchRecordsQuery};
use std::collections::BTreeMap;

impl crate::InboxQuery {
    pub fn from_query(query: &str) -> Result<Self, InvalidQuery> {
        let values = parse(query, &["state", "cursor", "limit"])?;
        Ok(Self {
            state: match values.get("state").map(String::as_str) {
                None | Some("unacknowledged") => crate::InboxState::Unacknowledged,
                Some("acknowledged") => crate::InboxState::Acknowledged,
                Some("all") => crate::InboxState::All,
                _ => return Err(InvalidQuery),
            },
            page: page(&values)?,
        })
    }

    pub fn pairs(&self) -> Vec<(String, String)> {
        let mut pairs = self.page.pairs();
        pairs.push(("state".into(), self.state.as_str().into()));
        pairs
    }
}

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

impl SearchRecordsQuery {
    pub fn from_query(query: &str) -> Result<Self, InvalidQuery> {
        let values = parse(
            query,
            &[
                "q",
                "cursor",
                "limit",
                "author",
                "attention",
                "since",
                "order",
            ],
        )?;
        let result = Self {
            q: values.get("q").cloned().ok_or(InvalidQuery)?,
            page: page(&values)?,
            author: values.get("author").cloned(),
            attention: values.get("attention").cloned(),
            since: values.get("since").cloned(),
            order: match values.get("order").map(String::as_str) {
                None | Some("rank") => SearchOrder::Rank,
                Some("seq") => SearchOrder::Seq,
                _ => return Err(InvalidQuery),
            },
        };
        result.validate().map_err(|_| InvalidQuery)?;
        Ok(result)
    }

    pub fn pairs(&self) -> Vec<(String, String)> {
        let mut pairs = self.page.pairs();
        pairs.push(("q".into(), self.q.clone()));
        pairs.push((
            "order".into(),
            match self.order {
                SearchOrder::Rank => "rank",
                SearchOrder::Seq => "seq",
            }
            .into(),
        ));
        for (key, value) in [
            ("author", &self.author),
            ("attention", &self.attention),
            ("since", &self.since),
        ] {
            if let Some(value) = value {
                pairs.push((key.into(), value.clone()));
            }
        }
        pairs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn search_roundtrip_and_boundaries() {
        let query = SearchRecordsQuery::from_query("q=hello+%22world%22&order=seq&since=2026-01-01T00%3A00%3A00%2B01%3A00&attention=reader&author=writer&limit=100").unwrap();
        assert_eq!(
            SearchRecordsQuery::from_query(&query_string(&query.pairs())).unwrap(),
            query
        );
        assert!(SearchRecordsQuery::from_query(&format!("q={}", "界".repeat(512))).is_ok());
        for query in [
            "",
            "q=",
            "q=a&q=b",
            "q=a&order=wrong",
            "q=a&limit=0",
            "q=a&limit=101",
            "q=a&since=yesterday",
            "q=%ff",
            "q=%xx",
            "q=a&after_seq=1",
            "q=a&cursor=x&cursor=y",
            "q=a&unexpected=x",
        ] {
            assert!(SearchRecordsQuery::from_query(query).is_err(), "{query}");
        }
        assert!(SearchRecordsQuery::from_query(&format!("q={}", "界".repeat(513))).is_err());
    }

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
