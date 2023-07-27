use crate::{
    codec::Codec,
    message::{TryFromRawHeaders, CONTENT_TYPE_HEADER, KIND_HEADER},
    Message, RawHeaders, RawMessage,
};

use super::Extensions;

use thiserror::Error;

pub struct ProcessContext<'a> {
    pub(crate) payload: &'a [u8],
    pub(crate) headers: RawHeaders,
    pub(crate) extensions: &'a Extensions,
}

#[derive(Debug, Error)]
pub enum MessageConvertError {
    #[error("Codec error: {0}")]
    Codec(Box<dyn std::error::Error + Send + Sync>),

    #[error("Headers decode error: {0}")]
    Headers(Box<dyn std::error::Error + Send + Sync>),
}

impl<'a> ProcessContext<'a> {
    pub fn kind(&self) -> Option<&str> {
        self.headers.get(KIND_HEADER).map(|hdr| hdr.as_str())
    }

    pub fn content_type(&self) -> Option<&str> {
        self.headers
            .get(CONTENT_TYPE_HEADER)
            .map(|hdr| hdr.as_str())
    }

    pub fn into_owned(self) -> RawMessage {
        RawMessage {
            payload: self.payload.to_vec(),
            headers: self.headers,
        }
    }

    pub(crate) fn into_message<M, C>(self, codec: C) -> Result<M, MessageConvertError>
    where
        M: Message,
        C: Codec,
    {
        let Self {
            payload,
            headers,
            extensions: _,
        } = self;

        let headers: M::Headers = TryFromRawHeaders::try_from_raw_headers(headers)
            .map_err(|err| MessageConvertError::Headers(Box::new(err)))?;

        let body: M::Body = codec
            .decode(payload)
            .map_err(|err| MessageConvertError::Codec(Box::new(err)))?;

        Ok(Message::from_body_and_headers(body, headers))
    }
}
