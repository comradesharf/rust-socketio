use base64::{Engine as _, engine::general_purpose};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::char;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::{Display, Formatter, Result as FmtResult, Write};
use std::ops::Index;

use crate::error::{Error, Result};
/// Enumeration of the `engine.io` `Packet` types.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum PacketId {
    Open,
    Close,
    Ping,
    Pong,
    Message,
    // A type of message that is base64 encoded
    MessageBinary,
    Upgrade,
    Noop,
}

impl PacketId {
    /// Returns the byte that represents the [`PacketId`] as a [`char`].
    fn to_string_byte(self) -> u8 {
        match self {
            Self::MessageBinary => b'b',
            _ => u8::from(self) + b'0',
        }
    }
}

impl Display for PacketId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_char(self.to_string_byte() as char)
    }
}

impl From<PacketId> for u8 {
    fn from(packet_id: PacketId) -> Self {
        match packet_id {
            PacketId::Open => 0,
            PacketId::Close => 1,
            PacketId::Ping => 2,
            PacketId::Pong => 3,
            PacketId::Message => 4,
            PacketId::MessageBinary => 4,
            PacketId::Upgrade => 5,
            PacketId::Noop => 6,
        }
    }
}

impl TryFrom<u8> for PacketId {
    type Error = Error;
    /// Converts a byte into the corresponding `PacketId`.
    fn try_from(b: u8) -> Result<PacketId> {
        match b {
            0 | b'0' => Ok(PacketId::Open),
            1 | b'1' => Ok(PacketId::Close),
            2 | b'2' => Ok(PacketId::Ping),
            3 | b'3' => Ok(PacketId::Pong),
            4 | b'4' => Ok(PacketId::Message),
            5 | b'5' => Ok(PacketId::Upgrade),
            6 | b'6' => Ok(PacketId::Noop),
            _ => Err(Error::InvalidPacketId(b)),
        }
    }
}

/// A `Packet` sent via the `engine.io` protocol.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Packet {
    pub packet_id: PacketId,
    pub data: Bytes,
}

/// Data which gets exchanged in a handshake as defined by the server.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct HandshakePacket {
    pub sid: String,
    pub upgrades: Vec<String>,
    #[serde(rename = "pingInterval")]
    pub ping_interval: u64,
    #[serde(rename = "pingTimeout")]
    pub ping_timeout: u64,
}

impl TryFrom<Packet> for HandshakePacket {
    type Error = Error;
    fn try_from(packet: Packet) -> Result<HandshakePacket> {
        Ok(serde_json::from_slice(packet.data[..].as_ref())?)
    }
}

impl Packet {
    /// Creates a new `Packet`.
    pub fn new<T: Into<Bytes>>(packet_id: PacketId, data: T) -> Self {
        Packet {
            packet_id,
            data: data.into(),
        }
    }
}

impl TryFrom<Bytes> for Packet {
    type Error = Error;
    /// Decodes a single `Packet` from an `u8` byte stream.
    fn try_from(
        bytes: Bytes,
    ) -> std::result::Result<Self, <Self as std::convert::TryFrom<Bytes>>::Error> {
        if bytes.is_empty() {
            return Err(Error::IncompletePacket());
        }

        let is_base64 = *bytes.first().ok_or(Error::IncompletePacket())? == b'b';

        // only 'messages' packets could be encoded
        let packet_id = if is_base64 {
            PacketId::MessageBinary
        } else {
            (*bytes.first().ok_or(Error::IncompletePacket())?).try_into()?
        };

        if bytes.len() == 1 && packet_id == PacketId::Message {
            return Err(Error::IncompletePacket());
        }

        let data: Bytes = bytes.slice(1..);

        Ok(Packet {
            packet_id,
            data: if is_base64 {
                Bytes::from(general_purpose::STANDARD.decode(data.as_ref())?)
            } else {
                data
            },
        })
    }
}

impl Packet {
    fn encoded_len(&self) -> usize {
        1 + if self.packet_id == PacketId::MessageBinary {
            base64::encoded_len(self.data.len(), true).expect("packet too large")
        } else {
            self.data.len()
        }
    }

    fn encode_into(&self, buffer: &mut BytesMut) {
        buffer.put_u8(self.packet_id.to_string_byte());
        if self.packet_id == PacketId::MessageBinary {
            let start = buffer.len();
            buffer.resize(start + self.encoded_len() - 1, 0);
            general_purpose::STANDARD
                .encode_slice(&self.data, &mut buffer[start..])
                .expect("buffer has the exact base64 encoded length");
        } else {
            buffer.extend_from_slice(&self.data);
        }
    }
}

impl From<Packet> for Bytes {
    /// Encodes a packet directly into a single allocation.
    fn from(packet: Packet) -> Self {
        let mut buffer = BytesMut::with_capacity(packet.encoded_len());
        packet.encode_into(&mut buffer);
        buffer.freeze()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Payload(Vec<Packet>);

impl Payload {
    // see https://en.wikipedia.org/wiki/Delimiter#ASCII_delimited_text
    const SEPARATOR: u8 = b'\x1e';

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl TryFrom<Bytes> for Payload {
    type Error = Error;
    /// Decodes a `payload` which in the `engine.io` context means a chain of normal
    /// packets separated by a certain SEPARATOR, in this case the delimiter `\x1e`.
    fn try_from(payload: Bytes) -> Result<Self> {
        payload
            .split(|&c| c == Self::SEPARATOR)
            .map(|slice| Packet::try_from(payload.slice_ref(slice)))
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }
}

impl TryFrom<Payload> for Bytes {
    type Error = Error;
    /// Encodes a payload. Payload in the `engine.io` context means a chain of
    /// normal `packets` separated by a SEPARATOR, in this case the delimiter
    /// `\x1e`.
    fn try_from(packets: Payload) -> Result<Self> {
        let capacity = packets.0.iter().map(Packet::encoded_len).sum::<usize>()
            + packets.0.len().saturating_sub(1);
        let mut buf = BytesMut::with_capacity(capacity);
        for (index, packet) in packets.0.iter().enumerate() {
            if index > 0 {
                buf.put_u8(Payload::SEPARATOR);
            }
            packet.encode_into(&mut buf);
        }

        Ok(buf.freeze())
    }
}

#[derive(Clone, Debug)]
pub struct IntoIter {
    iter: std::vec::IntoIter<Packet>,
}

impl Iterator for IntoIter {
    type Item = Packet;
    fn next(&mut self) -> std::option::Option<<Self as std::iter::Iterator>::Item> {
        self.iter.next()
    }
}

impl IntoIterator for Payload {
    type Item = Packet;
    type IntoIter = IntoIter;
    fn into_iter(self) -> <Self as std::iter::IntoIterator>::IntoIter {
        IntoIter {
            iter: self.0.into_iter(),
        }
    }
}

impl Index<usize> for Payload {
    type Output = Packet;
    fn index(&self, index: usize) -> &Packet {
        &self.0[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_mixed_payload_encoding() {
        assert!(Bytes::try_from(Payload(Vec::new())).unwrap().is_empty());
        let packets = vec![
            Packet::new(PacketId::Ping, Bytes::new()),
            Packet::new(PacketId::Message, Bytes::from_static(b"hello")),
            Packet::new(PacketId::MessageBinary, Bytes::from_static(b"\x00\xff")),
        ];
        let encoded = Bytes::try_from(Payload(packets.clone())).unwrap();
        assert_eq!(encoded, Bytes::from_static(b"2\x1e4hello\x1ebAP8="));
        assert_eq!(Payload::try_from(encoded).unwrap().0, packets);
    }

    #[test]
    fn binary_encoding_handles_all_padding_lengths() {
        for size in [0, 1, 2, 3, 4, 1024, 65536] {
            let packet = Packet::new(PacketId::MessageBinary, vec![0xff; size]);
            let encoded = Bytes::from(packet.clone());
            assert_eq!(encoded.len(), 1 + base64::encoded_len(size, true).unwrap());
            assert_eq!(Packet::try_from(encoded).unwrap(), packet);
        }
    }

    #[test]
    fn test_packet_error() {
        let err = Packet::try_from(BytesMut::with_capacity(10).freeze());
        assert!(err.is_err())
    }

    #[test]
    fn test_is_reflexive() {
        let data = Bytes::from_static(b"1Hello World");
        let packet = Packet::try_from(data).unwrap();

        assert_eq!(packet.packet_id, PacketId::Close);
        assert_eq!(packet.data, Bytes::from_static(b"Hello World"));

        let data = Bytes::from_static(b"1Hello World");
        assert_eq!(Bytes::from(packet), data);
    }

    #[test]
    fn test_binary_packet() {
        // SGVsbG8= is the encoded string for 'Hello'
        let data = Bytes::from_static(b"bSGVsbG8=");
        let packet = Packet::try_from(data.clone()).unwrap();

        assert_eq!(packet.packet_id, PacketId::MessageBinary);
        assert_eq!(packet.data, Bytes::from_static(b"Hello"));

        assert_eq!(Bytes::from(packet), data);
    }

    #[test]
    fn test_decode_payload() -> Result<()> {
        let data = Bytes::from_static(b"1Hello\x1e1HelloWorld");
        let packets = Payload::try_from(data)?;

        assert_eq!(packets[0].packet_id, PacketId::Close);
        assert_eq!(packets[0].data, Bytes::from_static(b"Hello"));
        assert_eq!(packets[1].packet_id, PacketId::Close);
        assert_eq!(packets[1].data, Bytes::from_static(b"HelloWorld"));

        let data = "1Hello\x1e1HelloWorld".to_owned().into_bytes();
        assert_eq!(Bytes::try_from(packets).unwrap(), data);

        Ok(())
    }

    #[test]
    fn test_binary_payload() {
        let data = Bytes::from_static(b"bSGVsbG8=\x1ebSGVsbG9Xb3JsZA==\x1ebSGVsbG8=");
        let packets = Payload::try_from(data.clone()).unwrap();

        assert!(packets.len() == 3);
        assert_eq!(packets[0].packet_id, PacketId::MessageBinary);
        assert_eq!(packets[0].data, Bytes::from_static(b"Hello"));
        assert_eq!(packets[1].packet_id, PacketId::MessageBinary);
        assert_eq!(packets[1].data, Bytes::from_static(b"HelloWorld"));
        assert_eq!(packets[2].packet_id, PacketId::MessageBinary);
        assert_eq!(packets[2].data, Bytes::from_static(b"Hello"));

        assert_eq!(Bytes::try_from(packets).unwrap(), data);
    }

    #[test]
    fn test_packet_id_conversion_and_incompl_packet() -> Result<()> {
        let sut = Packet::try_from(Bytes::from_static(b"4"));
        assert!(sut.is_err());
        let _sut = sut.unwrap_err();
        assert!(matches!(Error::IncompletePacket, _sut));

        assert_eq!(PacketId::MessageBinary.to_string(), "b");

        let sut = PacketId::try_from(b'0')?;
        assert_eq!(sut, PacketId::Open);
        assert_eq!(sut.to_string(), "0");

        let sut = PacketId::try_from(b'1')?;
        assert_eq!(sut, PacketId::Close);
        assert_eq!(sut.to_string(), "1");

        let sut = PacketId::try_from(b'2')?;
        assert_eq!(sut, PacketId::Ping);
        assert_eq!(sut.to_string(), "2");

        let sut = PacketId::try_from(b'3')?;
        assert_eq!(sut, PacketId::Pong);
        assert_eq!(sut.to_string(), "3");

        let sut = PacketId::try_from(b'4')?;
        assert_eq!(sut, PacketId::Message);
        assert_eq!(sut.to_string(), "4");

        let sut = PacketId::try_from(b'5')?;
        assert_eq!(sut, PacketId::Upgrade);
        assert_eq!(sut.to_string(), "5");

        let sut = PacketId::try_from(b'6')?;
        assert_eq!(sut, PacketId::Noop);
        assert_eq!(sut.to_string(), "6");

        let sut = PacketId::try_from(42);
        assert!(sut.is_err());
        assert!(matches!(sut.unwrap_err(), Error::InvalidPacketId(42)));

        Ok(())
    }

    #[test]
    fn test_handshake_packet() {
        assert!(
            HandshakePacket::try_from(Packet::new(PacketId::Message, Bytes::from("test"))).is_err()
        );
        let packet = HandshakePacket {
            ping_interval: 10000,
            ping_timeout: 1000,
            sid: "Test".to_owned(),
            upgrades: vec!["websocket".to_owned(), "test".to_owned()],
        };
        let encoded: String = serde_json::to_string(&packet).unwrap();

        assert_eq!(
            packet,
            HandshakePacket::try_from(Packet::new(PacketId::Message, Bytes::from(encoded)))
                .unwrap()
        );
    }
}
