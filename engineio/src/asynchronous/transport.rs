use crate::error::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
#[cfg(feature = "async")]
use std::pin::Pin;
use url::Url;

use super::async_transports::{PollingTransport, WebsocketSecureTransport, WebsocketTransport};

// async-trait adds must_use to futures, which Clippy 1.99 flags redundantly.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait AsyncTransport: Stream<Item = Result<Bytes>> + Unpin {
    /// Sends a packet to the server. This optionally handles sending of a
    /// socketio binary attachment via the boolean attribute `is_binary_att`.
    async fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()>;

    /// Returns start of the url. ex. http://localhost:2998/engine.io/?EIO=4&transport=polling
    /// Must have EIO and transport already set.
    async fn base_url(&self) -> Result<Url>;

    /// Used to update the base path, like when adding the sid.
    async fn set_base_url(&self, base_url: Url) -> Result<()>;

    /// Full query address
    async fn address(&self) -> Result<Url>
    where
        Self: Sized,
    {
        Ok(crate::transport::cache_busted_url(self.base_url().await?))
    }
}

#[derive(Debug, Clone)]
pub enum AsyncTransportType {
    Polling(PollingTransport),
    Websocket(WebsocketTransport),
    WebsocketSecure(WebsocketSecureTransport),
}

impl From<PollingTransport> for AsyncTransportType {
    fn from(transport: PollingTransport) -> Self {
        AsyncTransportType::Polling(transport)
    }
}

impl From<WebsocketTransport> for AsyncTransportType {
    fn from(transport: WebsocketTransport) -> Self {
        AsyncTransportType::Websocket(transport)
    }
}

impl From<WebsocketSecureTransport> for AsyncTransportType {
    fn from(transport: WebsocketSecureTransport) -> Self {
        AsyncTransportType::WebsocketSecure(transport)
    }
}

#[cfg(feature = "async")]
impl AsyncTransportType {
    pub fn as_transport(&self) -> &(dyn AsyncTransport + Send) {
        match self {
            AsyncTransportType::Polling(transport) => transport,
            AsyncTransportType::Websocket(transport) => transport,
            AsyncTransportType::WebsocketSecure(transport) => transport,
        }
    }

    pub fn as_pin_box(&mut self) -> Pin<Box<&mut (dyn AsyncTransport + Send)>> {
        match self {
            AsyncTransportType::Polling(transport) => Box::pin(transport),
            AsyncTransportType::Websocket(transport) => Box::pin(transport),
            AsyncTransportType::WebsocketSecure(transport) => Box::pin(transport),
        }
    }
}
