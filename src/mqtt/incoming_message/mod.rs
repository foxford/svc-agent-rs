use super::*;

/// A generic received message.
#[derive(Debug, Clone)]
pub enum IncomingMessage<T> {
    Event(IncomingEvent<T>),
    Request(IncomingRequest<T>),
    Response(IncomingResponse<T>),
}

#[derive(Debug, Clone)]
pub struct IncomingMessageContent<T, P>
where
    P: Addressable + serde::Serialize,
{
    payload: T,
    properties: P,
}

impl<T, P> IncomingMessageContent<T, P>
where
    P: Addressable + serde::Serialize,
{
    pub fn new(payload: T, properties: P) -> Self {
        Self {
            payload,
            properties,
        }
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }

    pub fn extract_payload(self) -> T {
        self.payload
    }

    pub fn properties(&self) -> &P {
        &self.properties
    }

    pub fn properties_mut(&mut self) -> &mut P {
        &mut self.properties
    }
}

pub use incoming_event::*;
pub use incoming_request::*;
pub use incoming_response::*;

mod incoming_event;
mod incoming_request;
mod incoming_response;
