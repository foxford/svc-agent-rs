use chrono::{DateTime, Utc};
use serde_derive::Serialize;

use crate::serde::ts_milliseconds_string_option;
use crate::Addressable;
use crate::AgentId;
use crate::Authenticable;

use super::*;

/// Properties of an outgoing request.
#[derive(Debug, Serialize)]
pub struct OutgoingRequestProperties {
    method: String,
    correlation_data: String,
    response_topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_id: Option<AgentId>,
    #[serde(flatten)]
    long_term_timing: Option<LongTermTimingProperties>,
    #[serde(flatten)]
    short_term_timing: OutgoingShortTermTimingProperties,
    #[serde(flatten)]
    tracking: Option<TrackingProperties>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "ts_milliseconds_string_option"
    )]
    local_timestamp: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_tracking_label: Option<String>,
    #[serde(skip)]
    tags: ExtraTags,
}

impl OutgoingRequestProperties {
    /// Builds [OutgoingRequestProperties](struct.OutgoingRequestProperties.html).
    ///
    /// Use this function only if you're making a request from scratch.
    ///
    /// If you make a request while handling another request consider using
    /// [IncomingRequestProperties::to_request](struct.IncomingRequestProperties.html#method.to_request).
    ///
    /// # Arguments
    ///
    /// * `method` – request method.
    /// * `response_topic` – a topic to send the response to the request to.
    /// * `correlation_data` – any string to correlate request with the upcoming response.
    /// * `short_term_timing` – outgoing request's short term timing properties.
    ///
    /// # Example
    ///
    /// ```
    /// let props = OutgoingRequestProperties::new(
    ///     "system.vacuum",
    ///     &Subscription::unicast_responses(),
    ///     OutgoingShortTermTimingProperties::new(Utc::now()),
    /// );
    /// ```
    pub fn new(
        method: &str,
        response_topic: &str,
        correlation_data: &str,
        short_term_timing: OutgoingShortTermTimingProperties,
    ) -> Self {
        Self {
            method: method.to_owned(),
            response_topic: response_topic.to_owned(),
            correlation_data: correlation_data.to_owned(),
            agent_id: None,
            long_term_timing: None,
            short_term_timing,
            tracking: None,
            local_timestamp: None,
            local_tracking_label: None,
            tags: Default::default(),
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

    pub fn set_local_tracking_label(&mut self, local_tracking_label: String) -> &mut Self {
        self.local_tracking_label = Some(local_tracking_label);
        self
    }

    pub fn set_local_timestamp(&mut self, local_timestamp: DateTime<Utc>) -> &mut Self {
        self.local_timestamp = Some(local_timestamp);
        self
    }

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
    }

    pub fn tags(&self) -> &ExtraTags {
        &self.tags
    }

    pub fn set_tags(&mut self, tags: ExtraTags) -> &mut Self {
        self.tags = tags;
        self
    }
}

pub type OutgoingRequest<T> = OutgoingMessageContent<T, OutgoingRequestProperties>;

impl<T> OutgoingRequest<T>
where
    T: serde::Serialize,
{
    /// Builds a multicast request to publish.
    ///
    /// # Arguments
    ///
    /// * `payload` – any serializable value.
    /// * `properties` – properties of the outgoing request.
    /// * `to` – destination [AccountId](../struct.AccountId.html).
    ///
    /// # Example
    ///
    /// ```
    /// let props = request.properties().to_request(
    ///     "room.enter",
    ///     &Subscription::unicast_responses(),
    ///     "some_corr_data",
    ///     OutgoingShortTermTimingProperties::until_now(start_timestamp),
    /// );
    ///
    /// let to = AccountId::new("service_name", "svc.example.org");
    /// let message = OutgoingRequest::multicast(json!({ "foo": "bar" }), props, &to);
    /// ```
    pub fn multicast<A>(
        payload: T,
        properties: OutgoingRequestProperties,
        to: &A,
    ) -> OutgoingMessage<T>
    where
        A: Authenticable,
    {
        OutgoingMessage::Request(Self::new(
            payload,
            properties,
            Destination::Multicast(to.as_account_id().to_owned()),
        ))
    }

    /// Builds a unicast request to publish.
    ///
    /// # Arguments
    ///
    /// * `payload` – any serializable value.
    /// * `properties` – properties of the outgoing request.
    /// * `to` – destination [AgentId](../struct.AgentId.html).
    /// * `version` – destination agent's API version.
    ///
    /// # Example
    ///
    /// ```
    /// let props = request.properties().to_request(
    ///     "room.enter",
    ///     &Subscription::unicast_responses(),
    ///     "some_corr_data",
    ///     OutgoingShortTermTimingProperties::until_now(start_timestamp),
    /// );
    ///
    /// let to = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let message = OutgoingRequest::unicast(json!({ "foo": "bar" }), props, to, "v1");
    /// ```
    pub fn unicast<A>(
        payload: T,
        properties: OutgoingRequestProperties,
        to: &A,
        version: &str,
    ) -> OutgoingMessage<T>
    where
        A: Addressable,
    {
        OutgoingMessage::Request(Self::new(
            payload,
            properties,
            Destination::Unicast(to.as_agent_id().to_owned(), version.to_owned()),
        ))
    }
}

impl<T: serde::Serialize> Publishable for OutgoingRequest<T> {
    fn destination_topic(&self, publisher: &Address) -> Result<String, Error> {
        match self.destination {
            Destination::Unicast(ref agent_id, ref version) => Ok(format!(
                "agents/{agent_id}/api/{version}/in/{app}",
                agent_id = agent_id,
                version = version,
                app = publisher.id().as_account_id(),
            )),
            Destination::Multicast(ref account_id) => Ok(format!(
                "agents/{agent_id}/api/{version}/out/{app}",
                agent_id = publisher.id(),
                version = publisher.version(),
                app = account_id,
            )),
            _ => Err(Error::new(&format!(
                "destination = '{:?}' is incompatible with request message type",
                self.destination,
            ))),
        }
    }

    fn qos(&self) -> QoS {
        QoS::AtMostOnce
    }
}
