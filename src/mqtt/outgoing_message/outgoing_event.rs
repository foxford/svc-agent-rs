use serde_derive::Serialize;

use crate::AgentId;

use super::*;
use crate::Authenticable;

/// Properties of an outgoing event.
#[derive(Debug, Serialize)]
pub struct OutgoingEventProperties {
    label: &'static str,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_id: Option<AgentId>,
    #[serde(flatten)]
    long_term_timing: Option<LongTermTimingProperties>,
    #[serde(flatten)]
    short_term_timing: OutgoingShortTermTimingProperties,
    #[serde(flatten)]
    tracking: Option<TrackingProperties>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_tracking_label: Option<String>,
}

impl OutgoingEventProperties {
    /// Builds [OutgoingEventProperties](struct.OutgoingEventProperties.html).
    ///
    /// Use this function only if you're dispatching an event from scratch.
    ///
    /// If you make a reaction event on an incoming request or another event consider using
    /// [IncomingRequestProperties::to_event](struct.IncomingRequestProperties.html#method.to_event)
    /// or [IncomingEventProperties::to_event](struct.IncomingEventProperties.html#method.to_event)
    /// respectively.
    ///
    /// # Arguments
    ///
    /// * `label` – event label.
    /// * `short_term_timing` – event's short term timing properties.
    ///
    /// # Example
    ///
    /// ```
    /// let props = OutgoingEventProperties::new(
    ///     "agent.enter",
    ///     OutgoingShortTermTimingProperties::new(Utc::now()),
    /// );
    /// ```
    pub fn new(label: &'static str, short_term_timing: OutgoingShortTermTimingProperties) -> Self {
        Self {
            label,
            long_term_timing: None,
            short_term_timing,
            tracking: None,
            agent_id: None,
            local_tracking_label: None,
        }
    }

    pub fn set_agent_id(&mut self, agent_id: AgentId) -> &mut Self {
        self.agent_id = Some(agent_id);
        self
    }

    pub fn set_long_term_timing(&mut self, timing: LongTermTimingProperties) -> &mut Self {
        self.long_term_timing = Some(timing);
        self
    }

    pub fn set_tracking(&mut self, tracking: TrackingProperties) -> &mut Self {
        self.tracking = Some(tracking);
        self
    }

    pub fn set_local_tracking_label(&mut self, label: String) -> &mut Self {
        self.local_tracking_label = Some(label);
        self
    }
}

pub type OutgoingEvent<T> = OutgoingMessageContent<T, OutgoingEventProperties>;

impl<T> OutgoingEvent<T>
where
    T: serde::Serialize,
{
    /// Builds a broadcast event to publish.
    ///
    /// # Arguments
    ///
    /// * `payload` – any serializable value.
    /// * `properties` – properties of the outgoing event.
    /// * `to_uri` – broadcast resource path.
    /// See [Destination](../enum.Destination#variant.Broadcast) for details.
    ///
    /// # Example
    ///
    /// ```
    /// let short_term_timing = OutgoingShortTermTimingProperties::until_now(start_timestamp);
    ///
    /// let message = OutgoingEvent::broadcast(
    ///     json!({ "foo": "bar" }),
    ///     request.to_event("message.create", short_term_timing),
    ///     "rooms/123/events",
    /// );
    /// ```
    pub fn broadcast(
        payload: T,
        properties: OutgoingEventProperties,
        to_uri: &str,
    ) -> OutgoingMessage<T> {
        OutgoingMessage::Event(Self::new(
            payload,
            properties,
            Destination::Broadcast(to_uri.to_owned()),
        ))
    }

    /// Builds a multicast event to publish.
    ///
    /// # Arguments
    ///
    /// * `payload` – any serializable value.
    /// * `properties` – properties of the outgoing event.
    /// * `to` – destination [AccountId](../struct.AccountId.html).
    ///
    /// # Example
    ///
    /// ```
    /// let props = OutgoingEvent::multicast(
    ///     json!({ "foo": "bar" }),
    ///     request.to_event("message.create", short_term_timing),
    ///     ,
    /// );
    /// let to = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"))
    /// let message = OutgoingRequest::multicast(json!({ "foo": "bar" }), props, &to);
    /// ```
    pub fn multicast<A>(
        payload: T,
        properties: OutgoingEventProperties,
        to: &A,
    ) -> OutgoingMessage<T>
    where
        A: Authenticable,
    {
        OutgoingMessage::Event(Self::new(
            payload,
            properties,
            Destination::Multicast(to.as_account_id().to_owned()),
        ))
    }
}

impl<T: serde::Serialize> Publishable for OutgoingEvent<T> {
    fn destination_topic(&self, publisher: &Address) -> Result<String, Error> {
        match self.destination {
            Destination::Broadcast(ref uri) => Ok(format!(
                "apps/{app}/api/{version}/{uri}",
                app = publisher.id().as_account_id(),
                version = publisher.version(),
                uri = uri,
            )),
            Destination::Multicast(ref account_id) => Ok(format!(
                "agents/{agent_id}/api/{version}/out/{app}",
                agent_id = publisher.id(),
                version = publisher.version(),
                app = account_id,
            )),
            _ => Err(Error::new(&format!(
                "destination = '{:?}' is incompatible with event message type",
                self.destination,
            ))),
        }
    }

    fn qos(&self) -> QoS {
        QoS::AtLeastOnce
    }
}
