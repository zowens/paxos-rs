use serde::{de, Serializer, Deserializer};
use bytes::Bytes;
use std::fmt;

pub fn serialize<S: Serializer>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_bytes(value)
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
    deserializer.deserialize_byte_buf(BytesValueVisitor)
}

struct BytesValueVisitor;

impl<'de> de::Visitor<'de> for BytesValueVisitor {
    type Value = Bytes;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte array/string")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Bytes::copy_from_slice(v))
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Bytes::copy_from_slice(v))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.into())
    }
}
