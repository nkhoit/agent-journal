use journal_protocol::{Request, Response, Transport, TransportError};
use std::{io::Read, path::Path, time::Duration};

// A 100-record page can exceed 40 MiB when legal content needs JSON escaping.
const MAX_RESPONSE: u64 = 64 * 1024 * 1024;

pub struct HttpTransport {
    client: reqwest::blocking::Client,
    base: reqwest::Url,
    admin: bool,
}

impl HttpTransport {
    pub fn new(endpoint: &str) -> Result<Self, TransportError> {
        let base = reqwest::Url::parse(endpoint).map_err(|_| invalid())?;
        let loopback = matches!(base.host_str(), Some("127.0.0.1" | "[::1]" | "localhost"));
        if !(base.scheme() == "https" || (base.scheme() == "http" && loopback))
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.path() != "/"
        {
            return Err(invalid());
        }
        Ok(Self {
            client: builder().build().map_err(|_| invalid())?,
            base,
            admin: false,
        })
    }

    #[cfg(unix)]
    pub fn unix(socket: &Path) -> Result<Self, TransportError> {
        Ok(Self {
            client: builder()
                .unix_socket(socket)
                .build()
                .map_err(|_| invalid())?,
            base: reqwest::Url::parse("http://localhost").map_err(|_| invalid())?,
            admin: true,
        })
    }

    #[cfg(not(unix))]
    pub fn unix(_socket: &Path) -> Result<Self, TransportError> {
        Err(TransportError::Protocol(
            "protected Unix administration is unsupported on this platform".into(),
        ))
    }
}

fn builder() -> reqwest::blocking::ClientBuilder {
    reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .connect_timeout(Duration::from_secs(10))
        // Total-request timeout. This is load-bearing for long-poll claims:
        // the server may hold a claim for up to
        // `journal_domain::MAX_LONG_POLL_SECONDS` (30s) before the final
        // claim round-trip, so only ~5s of margin remains for proxy RTT and
        // server latency. If the client times out after the server already
        // committed a claim lease, the adapter treats the claim as failed and
        // escalates spool backoff while the item sits leased until expiry —
        // delayed delivery, never loss. Keep the margin in mind before
        // lowering this or raising the server bound.
        .timeout(Duration::from_secs(35))
}

fn invalid() -> TransportError {
    TransportError::Protocol("invalid HTTP transport configuration or response".into())
}

impl Transport for HttpTransport {
    fn send(&self, request: Request) -> Result<Response, TransportError> {
        if !request.path.starts_with("/v1/")
            || request.path.contains(['\r', '\n', '#'])
            || request.path.starts_with("/v1/admin/") != self.admin
        {
            return Err(invalid());
        }
        let url = self.base.join(&request.path).map_err(|_| invalid())?;
        if url.origin() != self.base.origin() || url.path().starts_with("/v1/admin/") != self.admin
        {
            return Err(invalid());
        }
        let method =
            reqwest::Method::from_bytes(request.method.as_bytes()).map_err(|_| invalid())?;
        let mut outgoing = self.client.request(method, url);
        for (name, value) in request.headers {
            outgoing = outgoing.header(name, value);
        }
        let incoming = outgoing
            .body(request.body)
            .send()
            .map_err(|_| TransportError::Unavailable)?;
        let status = incoming.status().as_u16();
        let request_id = incoming
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let mut body = Vec::new();
        incoming
            .take(MAX_RESPONSE + 1)
            .read_to_end(&mut body)
            .map_err(|_| TransportError::Unavailable)?;
        if body.len() as u64 > MAX_RESPONSE {
            return Err(invalid());
        }
        let mut response = Response::new(status, body);
        if let Some(request_id) = request_id {
            response.headers.insert("x-request-id".into(), request_id);
        }
        Ok(response)
    }
}
