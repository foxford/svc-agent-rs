pub use outgoing_event::*;
pub use outgoing_request::*;
pub use outgoing_response::*;

use super::*;

/// A generic received message.
#[derive(Debug)]
pub enum OutgoingMessage<T>
where
    T: serde::Serialize,
{
    Event(OutgoingEvent<T>),
    Request(OutgoingRequest<T>),
    Response(OutgoingResponse<T>),
}

#[derive(Debug)]
pub struct OutgoingMessageContent<T, P>
where
    T: serde::Serialize,
{
    pub(crate) payload: T,
    pub(crate) properties: P,
    pub(crate) destination: Destination,
}

impl<T, P> OutgoingMessageContent<T, P>
where
    T: serde::Serialize,
{
    pub(crate) fn new(payload: T, properties: P, destination: Destination) -> Self {
        Self {
            payload,
            properties,
            destination,
        }
    }

    pub fn properties(&self) -> &P {
        &self.properties
    }
}

impl<T: serde::Serialize> Publishable for OutgoingMessage<T> {
    fn destination_topic(&self, publisher: &Address) -> Result<String, Error> {
        match self {
            OutgoingMessage::Event(v) => v.destination_topic(publisher),
            OutgoingMessage::Response(v) => v.destination_topic(publisher),
            OutgoingMessage::Request(v) => v.destination_topic(publisher),
        }
    }

    fn qos(&self) -> QoS {
        match self {
            OutgoingMessage::Event(v) => v.qos(),
            OutgoingMessage::Response(v) => v.qos(),
            OutgoingMessage::Request(v) => v.qos(),
        }
    }
}

mod outgoing_event;
mod outgoing_request;
mod outgoing_response;
