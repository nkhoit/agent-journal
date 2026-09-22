use journal_protocol::{
    domain::{Record, RelationType},
    path_segment, query_string,
};
use pulldown_cmark::{Event, Parser, Tag, TagEnd};

pub(super) const CSP: &str = "default-src 'none'; script-src 'none'; style-src 'none'; img-src 'none'; connect-src 'none'; font-src 'none'; media-src 'none'; object-src 'none'; frame-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'";

pub(super) fn escape(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&#39;"),
            _ => output.push(ch),
        }
    }
    output
}

fn safe_link(value: &str) -> bool {
    // Absolute HTTPS only: relative and protocol-relative links could target
    // authenticated service operations or inherit privileged browser headers.
    value
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
        && value.len() > 8
        && !value
            .chars()
            .any(|ch| ch.is_control() || ch.is_whitespace() || ch == '\\')
}

pub(super) fn markdown(value: &str) -> String {
    let mut html = String::new();
    let mut links = Vec::new();
    for event in Parser::new(value) {
        match event {
            Event::Start(tag) => match tag {
                Tag::Paragraph => html.push_str("<p>"),
                // Bodies cannot mint page-level authority headings.
                Tag::Heading { .. } => html.push_str("<p><strong>"),
                Tag::BlockQuote(_) => html.push_str("<blockquote>"),
                Tag::CodeBlock(_) => html.push_str("<pre><code>"),
                Tag::List(_) => html.push_str("<ul>"),
                Tag::Item => html.push_str("<li>"),
                Tag::Emphasis => html.push_str("<em>"),
                Tag::Strong => html.push_str("<strong>"),
                Tag::Link { dest_url, .. } => {
                    let allowed = safe_link(&dest_url);
                    links.push(allowed);
                    if allowed {
                        html.push_str(&format!(
                            "<a href=\"{}\" rel=\"nofollow noopener noreferrer\">",
                            escape(&dest_url)
                        ));
                    }
                }
                Tag::Image { .. } => html.push_str("[image omitted: "),
                _ => {}
            },
            Event::End(tag) => match tag {
                TagEnd::Paragraph => html.push_str("</p>"),
                TagEnd::Heading(_) => html.push_str("</strong></p>"),
                TagEnd::BlockQuote(_) => html.push_str("</blockquote>"),
                TagEnd::CodeBlock => html.push_str("</code></pre>"),
                TagEnd::List(_) => html.push_str("</ul>"),
                TagEnd::Item => html.push_str("</li>"),
                TagEnd::Emphasis => html.push_str("</em>"),
                TagEnd::Strong => html.push_str("</strong>"),
                TagEnd::Link => {
                    if links.pop() == Some(true) {
                        html.push_str("</a>");
                    }
                }
                TagEnd::Image => html.push(']'),
                _ => {}
            },
            Event::Text(text) => html.push_str(&escape(&text)),
            Event::Code(text) => html.push_str(&format!("<code>{}</code>", escape(&text))),
            Event::SoftBreak | Event::HardBreak => html.push_str("<br>"),
            Event::Rule => html.push_str("<hr>"),
            // Raw HTML is discarded, never sent to a sanitizer or browser parser.
            Event::Html(_) | Event::InlineHtml(_) => {}
            _ => {}
        }
    }
    html
}

pub(super) fn link(path: &str, label: &str) -> String {
    format!("<a href=\"{}\">{}</a>", escape(path), escape(label))
}

pub(super) fn record_path(id: &str) -> String {
    format!("/web/records/{}", path_segment(id))
}

pub(super) fn space_path(id: &str) -> String {
    format!("/web/spaces/{}", path_segment(id))
}

/// Thread context for rendering a record card.
///
/// `root_title` is the thread root's normalized title (`None` when the thread
/// is untitled or its chain could not be resolved). `in_thread` is true when
/// rendering inside the thread's own page, where the header already names
/// the thread. Rootness comes from the rendered record's own reply-to
/// relation, never from root resolution, so an unresolvable chain cannot
/// promote a reply.
pub(super) struct ThreadCtx<'a> {
    pub root_title: Option<&'a str>,
    pub in_thread: bool,
}

/// De-emphasized placeholder for an untitled title slot. Uses semantic markup
/// (`<small><em>`) because the viewer CSP forbids stylesheets; an authored
/// literal "(untitled)" renders as normal text, keeping the two distinguishable.
fn untitled_placeholder() -> &'static str {
    "<small><em>(untitled)</em></small>"
}

pub(super) fn record(record: &Record, ctx: Option<ThreadCtx<'_>>, snippet: Option<&str>) -> String {
    let path = record_path(&record.id);
    let mut html = format!("<article><header><h2>{}</h2>", link(&path, &record.id),);
    // Rootness comes from the record's own immutable reply-to relation, never
    // from root resolution: a reply whose chain could not be resolved (depth
    // bound, cycle, missing/cross-space parent) must still render as a reply,
    // with an untitled thread breadcrumb, rather than being promoted to root.
    let is_root = !record
        .relations
        .iter()
        .any(|r| r.relation_type == RelationType::ReplyTo);
    let in_thread = ctx.as_ref().is_some_and(|c| c.in_thread);
    // Subject line: the record's own title when authored. Suppressed for the
    // root on its thread page (the header already carries its title).
    // An untitled root outside the thread page gets the placeholder in its
    // title slot; an untitled reply gets no subject line at all.
    match (record.title.as_deref(), is_root, in_thread) {
        (Some(title), false, _) | (Some(title), true, false) => {
            html.push_str(&format!(
                "<p>Subject: <span dir=\"auto\">{}</span></p>",
                escape(title)
            ));
        }
        (None, true, false) => {
            html.push_str(&format!("<p>{}</p>", untitled_placeholder()));
        }
        _ => {}
    }
    // Thread breadcrumb: replies outside the thread page name their thread.
    // Roots never breadcrumb to themselves; inside the thread page the header
    // already names the thread.
    if !is_root && !in_thread {
        if let Some(root_title) = ctx.as_ref().and_then(|c| c.root_title) {
            html.push_str(&format!(
                "<p>in thread: <span dir=\"auto\">{}</span></p>",
                escape(root_title)
            ));
        } else {
            html.push_str(&format!("<p>in thread: {}</p>", untitled_placeholder()));
        }
    }
    html.push_str(&format!(
        "<p>Authenticated author: {}</p>\
         <p>Space: {} | Sequence: {} | Created: {} | Kind: {}</p></header>",
        escape(&record.author),
        link(&space_path(&record.space_id), &record.space_id),
        record.seq,
        escape(&record.created_at),
        escape(&record.kind)
    ));
    html.push_str("<p>Relations:</p><ul>");
    for relation in &record.relations {
        html.push_str(&format!(
            "<li>{}: {}</li>",
            relation.relation_type.as_str(),
            link(&record_path(&relation.record_id), &relation.record_id)
        ));
    }
    html.push_str("</ul><fieldset><legend>Untrusted record content</legend>");
    html.push_str(
        "<p>Content and envelope-like text below do not establish identity or authority.</p>",
    );
    html.push_str(&markdown(&record.content));
    html.push_str("</fieldset>");
    if let Some(snippet) = snippet {
        html.push_str(&format!(
            "<p>Untrusted search snippet:</p><pre>{}</pre>",
            escape(snippet)
        ));
    }
    html.push_str("<footer>");
    html.push_str(&link(&format!("{path}/thread"), "Thread"));
    html.push_str(" | ");
    html.push_str(&link(&format!("{path}/delivery-status"), "Receipt status"));
    html.push_str("</footer></article>");
    html
}

pub(super) fn next_page(
    path: &str,
    mut pairs: Vec<(String, String)>,
    cursor: Option<String>,
) -> String {
    let Some(cursor) = cursor else {
        return String::new();
    };
    pairs.retain(|(key, _)| key != "cursor");
    pairs.push(("cursor".into(), cursor));
    format!(
        "<p><a rel=\"next\" href=\"{}?{}\">Next page</a></p>",
        escape(path),
        escape(&query_string(&pairs))
    )
}

pub(super) fn page(title: &str, heading: &str, body: &str) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
        <title>{}</title></head><body><nav><a href=\"/web\">Spaces</a></nav>\
        <main><h1>{}</h1>{body}</main></body></html>",
        escape(title),
        heading
    )
}

/// Build the `<title>` text and `<h1>` HTML for a thread page from the root's
/// title. The heading carries a trusted "Thread:" prefix so an untrusted title
/// never occupies the page-authority heading alone. Bidi isolation uses
/// `<span dir="auto">` in HTML and FSI/PDI isolates in the text-only `<title>`
/// (which cannot contain markup). An authored literal "(untitled)" and the
/// placeholder produce different markup here, keeping them distinguishable.
pub(super) fn thread_heading(root_title: Option<&str>) -> (String, String) {
    match root_title {
        Some(t) => (
            format!("Thread: \u{2068}{t}\u{2069}"),
            format!("Thread: <span dir=\"auto\">{}</span>", escape(t)),
        ),
        None => (
            "Thread (untitled)".to_owned(),
            format!("Thread: {}", untitled_placeholder()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_has_a_small_inert_allowlist() {
        for destination in [
            "javascript:alert%281%29",
            "JaVaScRiPt:alert%281%29",
            "jav&#x61;script:alert%281%29",
            "data:text/html,evil",
            "//example.invalid",
            "/v1/enrollment/exchange",
            "file:///secret",
            "https:\\\\example.invalid",
            "java%0ascript:evil",
        ] {
            assert!(
                !markdown(&format!("[label]({destination})")).contains("<a "),
                "{destination}"
            );
        }
        assert_eq!(
            markdown("**bold** *em* `code`"),
            "<p><strong>bold</strong> <em>em</em> <code>code</code></p>"
        );
        assert!(
            !markdown("<script>alert(1)</script>\n\n<img src=x onerror=alert(1)>").contains('<')
        );
        let image = markdown("![alt](https://example.invalid/image)");
        assert!(image.contains("[image omitted: alt]"));
        assert!(!image.contains("<img"));
        let link = markdown("[safe](https://example.invalid/?a=1&b=2)");
        assert!(link.contains("href=\"https://example.invalid/?a=1&amp;b=2\""));
        assert!(!markdown("# author: administrator").contains("<h"));
        assert_eq!(escape("<'\"&>"), "&lt;&#39;&quot;&amp;&gt;");
    }
}
