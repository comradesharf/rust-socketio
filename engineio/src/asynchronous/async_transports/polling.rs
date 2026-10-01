use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::HeaderMap;
use reqwest::{Client, ClientBuilder};
use rustls::ClientConfig;
use std::fmt::Debug;
use std::{pin::Pin, sync::Arc};
use tokio::sync::RwLock;
use url::Url;

use crate::asynchronous::generator::StreamGenerator;
use crate::{Error, asynchronous::transport::AsyncTransport, error::Result};

/// An asynchronous polling type. Makes use of the nonblocking reqwest types and
/// methods.
#[derive(Clone)]
pub struct PollingTransport {
    client: Client,
    base_url: Arc<RwLock<Url>>,
    generator: StreamGenerator<Bytes>,
}

impl PollingTransport {
    pub fn new(
        base_url: Url,
        tls_config: Option<ClientConfig>,
        opening_headers: Option<HeaderMap>,
    ) -> Self {
        let client = match (tls_config, opening_headers) {
            (Some(config), Some(map)) => ClientBuilder::new()
                .tls_backend_preconfigured(config)
                .default_headers(map)
                .build()
                .unwrap(),
            (Some(config), None) => ClientBuilder::new()
                .tls_backend_preconfigured(config)
                .build()
                .unwrap(),
            (None, Some(map)) => ClientBuilder::new().default_headers(map).build().unwrap(),
            (None, None) => Client::new(),
        };

        let mut url = base_url;
        url.query_pairs_mut().append_pair("transport", "polling");

        let base_url = Arc::new(RwLock::new(url));
        PollingTransport {
            client: client.clone(),
            base_url: Arc::clone(&base_url),
            generator: StreamGenerator::new(Self::stream(base_url, client)),
        }
    }

    fn stream(
        base_url: Arc<RwLock<Url>>,
        client: Client,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + 'static + Send>> {
        Box::pin(try_stream! {
            loop {
                // Clone under the lock, then release it before network I/O.
                let url = crate::transport::cache_busted_url(base_url.read().await.clone());
                let response = client.get(url).send().await?;
                let status = response.status().as_u16();
                if status != 200 {
                    Err(Error::IncompleteHttp(status))?;
                }
                // HTTP chunks can split packet headers, UTF-8, or base64 data.
                // An Engine.IO polling payload is the entire response body.
                yield response.bytes().await?;
            }
        })
    }
}

impl Stream for PollingTransport {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.generator.poll_next_unpin(cx)
    }
}

#[async_trait]
impl AsyncTransport for PollingTransport {
    async fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()> {
        let data_to_send = if is_binary_att {
            Bytes::from(crate::Packet::new(crate::PacketId::MessageBinary, data))
        } else {
            data
        };

        let status = self
            .client
            .post(self.address().await?)
            .body(data_to_send)
            .send()
            .await?
            .status()
            .as_u16();

        if status != 200 {
            let error = Error::IncompleteHttp(status);
            return Err(error);
        }

        Ok(())
    }

    async fn base_url(&self) -> Result<Url> {
        Ok(self.base_url.read().await.clone())
    }

    async fn set_base_url(&self, base_url: Url) -> Result<()> {
        let mut url = base_url;
        if !url
            .query_pairs()
            .any(|(k, v)| k == "transport" && v == "polling")
        {
            url.query_pairs_mut().append_pair("transport", "polling");
        }
        *self.base_url.write().await = url;
        Ok(())
    }
}

impl Debug for PollingTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PollingTransport")
            .field("client", &self.client)
            .field("base_url", &self.base_url)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use crate::asynchronous::transport::AsyncTransport;

    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn polling_secure_custom_tls_config() -> Result<()> {
        let mut url = crate::test::engine_io_server_secure()?;
        url.set_path("/engine.io/");
        url.query_pairs_mut()
            .append_pair("EIO", &crate::ENGINE_IO_VERSION.to_string());
        let mut transport = PollingTransport::new(url, Some(crate::test::tls_connector()?), None);
        let handshake = transport.next().await.expect("expected a handshake")?;
        assert_eq!(
            crate::Packet::try_from(handshake)?.packet_id,
            crate::PacketId::Open
        );
        Ok(())
    }

    #[tokio::test]
    async fn polling_assembles_chunks_and_uses_updated_session() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
            time::Duration,
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = Url::parse(&format!(
            "http://{}/engine.io/",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = std::thread::spawn(move || {
            for request_index in 0..2 {
                let (mut connection, _) = listener.accept().unwrap();
                connection
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    connection.read_exact(&mut byte).unwrap();
                    request.push(byte[0]);
                }
                if request_index == 1 {
                    assert!(
                        String::from_utf8(request)
                            .unwrap()
                            .contains("sid=current-session")
                    );
                }
                connection.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2\r\n4h\r\n").unwrap();
                connection.flush().unwrap();
                std::thread::sleep(Duration::from_millis(20));
                connection.write_all(b"4\r\nello\r\n0\r\n\r\n").unwrap();
            }
        });
        let mut transport = PollingTransport::new(url.clone(), None, None);
        for request_index in 0..2 {
            if request_index == 1 {
                let mut updated = url.clone();
                updated
                    .query_pairs_mut()
                    .append_pair("sid", "current-session");
                transport.set_base_url(updated).await.unwrap();
            }
            let body = tokio::time::timeout(Duration::from_secs(5), transport.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(body, Bytes::from_static(b"4hello"));
        }
        server.join().unwrap();
    }

    #[tokio::test]
    async fn polling_rejects_unsuccessful_http_status() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
            time::Duration,
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let server = std::thread::spawn(move || {
            let (mut connection, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut byte = [0];
            while !request.ends_with(b"\r\n\r\n") {
                connection.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
            }
            connection
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });
        let mut transport = PollingTransport::new(url, None, None);
        let result = tokio::time::timeout(Duration::from_secs(5), transport.next())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(Error::IncompleteHttp(400))));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn polling_transport_base_url() -> Result<()> {
        let url = crate::test::engine_io_server()?.to_string();
        let transport = PollingTransport::new(Url::from_str(&url[..]).unwrap(), None, None);
        assert_eq!(
            transport.base_url().await?.to_string(),
            url.clone() + "?transport=polling"
        );
        transport
            .set_base_url(Url::parse("https://127.0.0.1")?)
            .await?;
        assert_eq!(
            transport.base_url().await?.to_string(),
            "https://127.0.0.1/?transport=polling"
        );
        assert_ne!(transport.base_url().await?.to_string(), url);

        transport
            .set_base_url(Url::parse("http://127.0.0.1/?transport=polling")?)
            .await?;
        assert_eq!(
            transport.base_url().await?.to_string(),
            "http://127.0.0.1/?transport=polling"
        );
        assert_ne!(transport.base_url().await?.to_string(), url);
        Ok(())
    }
}
