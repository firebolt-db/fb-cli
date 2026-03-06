use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::header::HeaderMap;
use hyper::StatusCode;
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;

use crate::{FIREBOLT_PROTOCOL_VERSION, USER_AGENT};

/// HTTP/1.1 response received over a Unix domain socket.
pub struct UnixResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Incoming,
}

impl UnixResponse {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Read the next chunk from the response body, skipping empty data frames and trailers.
    pub async fn chunk(
        &mut self,
    ) -> Result<Option<Bytes>, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match self.body.frame().await {
                None => return Ok(None),
                Some(Err(e)) => return Err(Box::new(e)),
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        if !data.is_empty() {
                            return Ok(Some(data));
                        }
                        // empty data frame — keep reading
                    }
                    // trailer frame — keep reading
                }
            }
        }
    }

    pub async fn text(mut self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut s = String::new();
        while let Some(chunk) = self.chunk().await? {
            s.push_str(std::str::from_utf8(&chunk)?);
        }
        Ok(s)
    }
}

/// Unified response from either a TCP (reqwest) or Unix-socket (hyper) HTTP request.
pub enum Response {
    Http(reqwest::Response),
    Unix(UnixResponse),
}

impl Response {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Http(r) => r.status(),
            Self::Unix(r) => r.status(),
        }
    }

    pub fn headers(&self) -> &HeaderMap {
        match self {
            Self::Http(r) => r.headers(),
            Self::Unix(r) => r.headers(),
        }
    }

    /// Stream the next chunk from the response body.
    pub async fn chunk(
        &mut self,
    ) -> Result<Option<Bytes>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Http(r) => Ok(r.chunk().await?),
            Self::Unix(r) => r.chunk().await,
        }
    }

    /// Collect the entire body as a UTF-8 string.
    pub async fn text(self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Http(r) => Ok(r.text().await?),
            Self::Unix(r) => r.text().await,
        }
    }
}

/// Send a POST request, routing through a Unix domain socket when `unix_socket` is set,
/// or falling back to standard TCP via reqwest.
pub async fn post(
    url: &str,
    unix_socket: Option<&str>,
    body: String,
    authorization: Option<String>,
    machine_query: bool,
) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
    match unix_socket.filter(|s| !s.is_empty()) {
        Some(path) => unix_post(path, url, body, authorization, machine_query)
            .await
            .map(Response::Unix),
        None => tcp_post(url, body, authorization, machine_query)
            .await
            .map(Response::Http),
    }
}

async fn tcp_post(
    url: &str,
    body: String,
    authorization: Option<String>,
    machine_query: bool,
) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
    let mut request = reqwest::Client::builder()
        .http2_keep_alive_timeout(std::time::Duration::from_secs(3600))
        .http2_keep_alive_interval(Some(std::time::Duration::from_secs(60)))
        .http2_keep_alive_while_idle(false)
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
        .build()?
        .post(url)
        .header("user-agent", USER_AGENT)
        .header("Firebolt-Protocol-Version", FIREBOLT_PROTOCOL_VERSION);

    if machine_query {
        request = request.header("Firebolt-Machine-Query", "true");
    }
    if let Some(auth) = authorization {
        request = request.header("authorization", auth);
    }

    Ok(request.body(body).send().await?)
}

async fn unix_post(
    socket_path: &str,
    url: &str,
    body: String,
    authorization: Option<String>,
    machine_query: bool,
) -> Result<UnixResponse, Box<dyn std::error::Error + Send + Sync>> {
    // Extract path+query from the URL; the unix socket ignores host and port.
    let parsed = reqwest::Url::parse(url)?;
    let path_and_query = match parsed.query() {
        Some(q) => format!("{}?{}", parsed.path(), q),
        None => parsed.path().to_string(),
    };
    let host = parsed.host_str().unwrap_or("localhost");

    // Connect to the Unix domain socket.
    let stream = UnixStream::connect(socket_path).await?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = http1::handshake(io).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Build and send the HTTP/1.1 POST request.
    let mut builder = hyper::Request::builder()
        .method("POST")
        .uri(path_and_query)
        .header("host", host)
        .header("user-agent", USER_AGENT)
        .header("Firebolt-Protocol-Version", FIREBOLT_PROTOCOL_VERSION)
        .header("content-length", body.len().to_string());

    if machine_query {
        builder = builder.header("Firebolt-Machine-Query", "true");
    }
    if let Some(auth) = authorization {
        builder = builder.header("authorization", auth);
    }

    let request = builder.body(Full::new(Bytes::from(body)))?;
    let response = sender.send_request(request).await?;
    let (parts, body) = response.into_parts();

    Ok(UnixResponse {
        status: parts.status,
        headers: parts.headers,
        body,
    })
}
