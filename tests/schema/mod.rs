use convoy::message::{Message, RawHeaders, CONTENT_TYPE_HEADER, KIND_HEADER};
use fake::faker::name::en::Name;
use fake::Dummy;
use serde::{Deserialize, Serialize};

pub struct ModelContainer(pub Model, pub Headers);

use crate::in_memory::InMemoryMessage;

#[derive(Debug, Serialize, Deserialize, Dummy, PartialEq, Eq)]
pub struct Model {
    pub id: String,

    #[dummy(faker = "Name()")]
    pub name: String,

    #[dummy(faker = "1990..2024")]
    pub production_year: i64,
}

impl Model {
    pub fn marshal(&self) -> InMemoryMessage {
        let key = self.id.to_string();

        let payload = serde_json::to_vec(&self).unwrap();
        let mut headers = RawHeaders::default();
        headers.insert(
            CONTENT_TYPE_HEADER.to_owned(),
            "application/json".to_owned(),
        );
        headers.insert(KIND_HEADER.to_owned(), ModelContainer::KIND.to_owned());

        InMemoryMessage {
            key: Some(key),
            payload,
            headers,
        }
    }
}

impl Message for ModelContainer {
    const KIND: &'static str = "model-v1";

    type Body = Model;

    type Headers = Headers;

    fn from_body_and_headers(body: Self::Body, headers: Self::Headers) -> Self {
        Self(body, headers)
    }

    fn into_body_and_headers(self) -> (Self::Body, Self::Headers) {
        (self.0, self.1)
    }

    fn key(&self) -> String {
        self.0.id.clone()
    }
}

pub struct Headers;

impl From<RawHeaders> for Headers {
    fn from(_: RawHeaders) -> Self {
        Self
    }
}

impl From<Headers> for RawHeaders {
    fn from(_: Headers) -> Self {
        Self::default()
    }
}
