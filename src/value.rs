use bytes::Bytes;
use serde;
use serde::de;
use serde::ser;
use std::fmt;
use std::ops::Deref;

/// Command value that the cluster members agree upon to reach consensus.
///
/// TODO: trait aliases!
pub trait Value:
    ser::Serialize + de::DeserializeOwned + PartialEq + Eq + Clone + fmt::Debug + Send
{
}

/// Paxos `Value` that is an opaque array of bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BytesValue(pub Bytes);

impl Value for BytesValue {}

impl From<Vec<u8>> for BytesValue {
    fn from(vec: Vec<u8>) -> BytesValue {
        BytesValue(vec.into())
    }
}

impl From<String> for BytesValue {
    fn from(s: String) -> BytesValue {
        BytesValue(s.into())
    }
}

impl Deref for BytesValue {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}

impl serde::Serialize for BytesValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self)
    }
}

impl<'de> serde::Deserialize<'de> for BytesValue {
    fn deserialize<D>(deserializer: D) -> Result<BytesValue, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(BytesValueVisitor)
    }
}

struct BytesValueVisitor;

impl<'de> de::Visitor<'de> for BytesValueVisitor {
    type Value = BytesValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte array/string")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.to_vec().into())
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.to_vec().into())
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.into())
    }
}
