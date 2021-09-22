use super::*;
use crate::Error;

pub trait Publishable {
    /// Returns a destination topic as string.
    ///
    /// # Arguments
    ///
    /// * `publisher` – publisher agent.
    ///
    /// # Example
    ///
    /// ```
    /// let topic = message.destination_topic(&agent_id, "v1")?;
    /// ```
    fn destination_topic(&self, publisher: &Address) -> Result<String, Error>;

    /// Returns QoS for publishing.
    fn qos(&self) -> QoS;
}

/// Dumped version of [Publishable](trait.Publishable.html).
#[derive(Clone, Debug)]
pub struct PublishableDump {
    topic: String,
    qos: QoS,
    payload: String,
    tags: ExtraTags,
}

impl PublishableDump {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn qos(&self) -> QoS {
        self.qos
    }

    pub fn payload(&self) -> &str {
        &self.payload
    }

    pub fn tags(&self) -> &ExtraTags {
        &self.tags
    }
}

pub enum PublishableMessage {
    Event(PublishableDump),
    Request(PublishableDump),
    Response(PublishableDump),
}

impl PublishableMessage {
    pub fn topic(&self) -> &str {
        match self {
            Self::Event(v) => v.topic(),
            Self::Request(v) => v.topic(),
            Self::Response(v) => v.topic(),
        }
    }

    pub fn qos(&self) -> QoS {
        match self {
            Self::Event(v) => v.qos(),
            Self::Request(v) => v.qos(),
            Self::Response(v) => v.qos(),
        }
    }

    pub fn payload(&self) -> &str {
        match self {
            Self::Event(v) => v.payload(),
            Self::Request(v) => v.payload(),
            Self::Response(v) => v.payload(),
        }
    }

    pub fn tags(&self) -> &ExtraTags {
        match self {
            Self::Event(v) => v.tags(),
            Self::Request(v) => v.tags(),
            Self::Response(v) => v.tags(),
        }
    }
}

pub trait IntoPublishableMessage {
    /// Serializes the object into dump that can be directly published.
    fn into_dump(self: Box<Self>, publisher: &Address) -> Result<PublishableMessage, Error>;
}

impl<T: serde::Serialize> IntoPublishableMessage for OutgoingMessage<T> {
    fn into_dump(self: Box<Self>, publisher: &Address) -> Result<PublishableMessage, Error> {
        use crate::mqtt::compat::{IntoEnvelope, OutgoingEnvelopeProperties};

        let topic = self.destination_topic(publisher)?;
        let qos = self.qos();
        let tags = self.tags().to_owned();

        let envelope = &self.into_envelope()?;
        let payload = serde_json::to_string(envelope)
            .map_err(|e| Error::new(&format!("error serializing an envelope, {}", &e)))?;

        let dump = PublishableDump {
            topic,
            qos,
            payload,
            tags,
        };

        let message = match envelope.properties {
            OutgoingEnvelopeProperties::Event(_) => PublishableMessage::Event(dump),
            OutgoingEnvelopeProperties::Request(_) => PublishableMessage::Request(dump),
            OutgoingEnvelopeProperties::Response(_) => PublishableMessage::Response(dump),
        };

        Ok(message)
    }
}
