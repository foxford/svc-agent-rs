/// MQTT 3.1 compatibility utilities.
///
/// [mqtt-gateway](https://github.com/netology-group/mqtt-gateway) supports both MQTT 3.1 and MQTT 5
/// protocol versions. However svc-agent is based on [rumq](https://github.com/tekjar/rumq)
/// MQTT client which currently only supports MQTT 3.1.
///
/// MQTT 5 introduces message properties that are somewhat like HTTP headers.
/// An alternative for them in MQTT 3.1 is to send this data right in the message payload.
/// So [mqtt-gateway](https://github.com/netology-group/mqtt-gateway) supports the envelope
/// payload format convention which looks like this:
///
/// ```json
/// {
///     "payload": "{ … }",
///     "properties": {
///         "name": "value"
///     }
/// }
/// ```
///
/// `payload` is the payload itself. Even if it's a JSON object by itself it needs to be serialized
/// to string.
///
/// `properties` is an object of name-value pairs. All values must be strings.
///
/// [mqtt-gateway](https://github.com/netology-group/mqtt-gateway) does a translation between
/// MQTT 3.1 envelope and plain MQTT 5 formats so clients may talk to each other using
/// different versions of the protocol. When an MQTT 5 client sends a message to an MQTT 3.1 client
/// it sends plain payload and properties using MQTT 5 features but latter client receives
/// it in the envelope format. And vice versa: published an MQTT 3.1 envelope will be received
/// as a plain MQTT 5 message by an MQTT 5 client.
///
/// This module implements the agent's part of this convention.
///
/// Use [serde](http://github.com/serde-rs/serde) to parse an incoming envelope.
/// Here's an example on how to parse an incoming message:
///
/// ```
/// #[derive(DeserializeOwned)]
/// struct RoomEnterRequestPayload {
///     room_id: usize,
/// }
///
/// let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(payload)?;
///
/// match envelope.properties() {
///     compat::IncomingEnvelopeProperties::Request(ref reqp) => {
///         // Request routing by method property
///         match reqp.method() {
///             "room.enter" => match compat::into_request::<RoomEnterRequestPayload>(envelope) {
///                 Ok(request) => {
///                     // Handle request.
///                 }
///                 Err(err) => {
///                     // Bad request: failed to parse payload for this method.
///                 }
///             }
///         }
///     }
///     compat::IncomingEnvelopeProperties::Response(ref respp) => {
///         // The same for response.
///     }
///     compat::IncomingEnvelopeProperties::Response(ref respp) => {
///         // The same for events.
///     }
/// }
/// ```
///
/// Enveloping of outgoing messages is up to svc-agent.
/// Just use (Agent::publish)[../struct.Agent.html#method.publish] method to publish messages.
use serde::{Deserialize, Serialize};

use super::{
    Destination, IncomingEvent, IncomingEventProperties, IncomingMessage, IncomingRequest,
    IncomingRequestProperties, IncomingResponse, IncomingResponseProperties,
    OutgoingEventProperties, OutgoingRequestProperties, OutgoingResponseProperties,
};
use crate::Error;

////////////////////////////////////////////////////////////////////////////////

/// Enveloped properties of an incoming message.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub(crate) enum IncomingEnvelopeProperties {
    Event(IncomingEventProperties),
    Request(IncomingRequestProperties),
    Response(IncomingResponseProperties),
}

/// Incoming enveloped message.
#[derive(Debug, Deserialize)]
pub(crate) struct IncomingEnvelope {
    payload: String,
    properties: IncomingEnvelopeProperties,
}

impl IncomingEnvelope {
    pub(crate) fn properties(&self) -> &IncomingEnvelopeProperties {
        &self.properties
    }
}

/// Parses an incoming envelope as an event with payload of type `T`.
pub(crate) fn into_event(envelope: IncomingEnvelope) -> Result<IncomingMessage<String>, Error> {
    let payload = envelope.payload;
    match envelope.properties {
        IncomingEnvelopeProperties::Event(props) => {
            Ok(IncomingMessage::Event(IncomingEvent::new(payload, props)))
        }
        _ => Err(Error::new("error serializing an envelope into event")),
    }
}

/// Parses an incoming envelope as a request with payload of type `T`.
pub(crate) fn into_request(envelope: IncomingEnvelope) -> Result<IncomingMessage<String>, Error> {
    let payload = envelope.payload;
    match envelope.properties {
        IncomingEnvelopeProperties::Request(props) => Ok(IncomingMessage::Request(
            IncomingRequest::new(payload, props),
        )),
        _ => Err(Error::new("error serializing an envelope into request")),
    }
}

/// Parses an incoming envelope as a response with payload of type `T`.
pub(crate) fn into_response(envelope: IncomingEnvelope) -> Result<IncomingMessage<String>, Error> {
    let payload = envelope.payload;
    match envelope.properties {
        IncomingEnvelopeProperties::Response(props) => Ok(IncomingMessage::Response(
            IncomingResponse::new(payload, props),
        )),
        _ => Err(Error::new("error serializing an envelope into response")),
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Properties of an outgoing envelope.
#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum OutgoingEnvelopeProperties {
    Event(OutgoingEventProperties),
    Request(OutgoingRequestProperties),
    Response(OutgoingResponseProperties),
}

/// Outgoing enveloped message.
#[derive(Debug, Serialize)]
pub struct OutgoingEnvelope {
    payload: String,
    pub(crate) properties: OutgoingEnvelopeProperties,
    #[serde(skip)]
    destination: Destination,
}

impl OutgoingEnvelope {
    /// Builds an [OutgoingEnvelope](struct.OutgoingEnvelope.html).
    ///
    /// # Arguments
    ///
    /// * `payload` – payload serialized to string,
    /// * `properties` – enveloped properties.
    /// * `destination` – [Destination](../../enum.Destination.html) of the message.
    pub fn new(
        payload: &str,
        properties: OutgoingEnvelopeProperties,
        destination: Destination,
    ) -> Self {
        Self {
            payload: payload.to_owned(),
            properties,
            destination,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub trait IntoEnvelope {
    /// Wraps an outgoing message into envelope format.
    fn into_envelope(self) -> Result<OutgoingEnvelope, Error>;
}

////////////////////////////////////////////////////////////////////////////////

use super::OutgoingMessage;
impl<T> IntoEnvelope for super::OutgoingEvent<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)
            .map_err(|e| Error::new(&format!("error serializing payload of an envelope, {}", e)))?;
        let envelope = OutgoingEnvelope::new(
            &payload,
            OutgoingEnvelopeProperties::Event(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

impl<T> IntoEnvelope for super::OutgoingRequest<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)
            .map_err(|e| Error::new(&format!("error serializing payload of an envelope, {}", e)))?;
        let envelope = OutgoingEnvelope::new(
            &payload,
            OutgoingEnvelopeProperties::Request(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

impl<T> IntoEnvelope for super::OutgoingResponse<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)
            .map_err(|e| Error::new(&format!("error serializing payload of an envelope, {}", e)))?;
        let envelope = OutgoingEnvelope::new(
            &payload,
            OutgoingEnvelopeProperties::Response(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

impl<T> IntoEnvelope for OutgoingMessage<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<OutgoingEnvelope, Error> {
        match self {
            OutgoingMessage::Event(v) => v.into_envelope(),
            OutgoingMessage::Response(v) => v.into_envelope(),
            OutgoingMessage::Request(v) => v.into_envelope(),
        }
    }
}
