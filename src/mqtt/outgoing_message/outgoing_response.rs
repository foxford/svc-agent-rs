use serde_derive::Serialize;

use super::*;
use crate::Addressable;

/// Properties of an outgoing response.
#[derive(Debug, Serialize)]
pub struct OutgoingResponseProperties {
    #[serde(with = "crate::serde::HttpStatusCodeRef")]
    status: ResponseStatus,
    correlation_data: String,
    #[serde(skip)]
    response_topic: Option<String>,
    #[serde(flatten)]
    long_term_timing: LongTermTimingProperties,
    #[serde(flatten)]
    short_term_timing: OutgoingShortTermTimingProperties,
    #[serde(flatten)]
    tracking: TrackingProperties,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_tracking_label: Option<String>,
    #[serde(skip)]
    tags: ExtraTags,
}

impl OutgoingResponseProperties {
    /// Builds [OutgoingResponseProperties](struct.OutgoingResponseProperties.html).
    ///
    /// Generally you shouldn't use this function and consider using
    /// [IncomingRequestProperties::to_response](struct.IncomingRequestProperties.html#method.to_response)
    /// because all outgoing responses are related to an incoming request to respond to.
    /// However if you need to customize the response creation you may want to call this constructor
    /// directly.
    ///
    /// # Arguments
    ///
    /// * `status` – HTTP-compatible status code.
    /// * `correlation_data` – a correlation string between request and response.
    /// It has meaning to the sender of the request message and receiver of the response message.
    /// * `long_term_timing` – outgoing response's long term timing properties.
    /// * `short_term_timing` – outgoing response's short term timing properties.
    /// * `tracking_properties` – outgoing response's short term tracking properties.
    ///
    /// # Example
    ///
    /// ```
    /// let resp_props = OutgoingResponseProperties::new(
    ///     ResponseStatus::OK,
    ///     req_props.correlation_data().clone(),
    ///     req_props.long_term_timing().clone(),
    ///     OutgoingShortTermTimingProperties::new(Utc::now()),
    ///     req_props.tracking().clone(),
    /// );
    /// ```
    pub fn new(
        status: ResponseStatus,
        correlation_data: &str,
        long_term_timing: LongTermTimingProperties,
        short_term_timing: OutgoingShortTermTimingProperties,
        tracking: TrackingProperties,
        local_tracking_label: Option<String>,
    ) -> Self {
        Self {
            status,
            correlation_data: correlation_data.to_owned(),
            response_topic: None,
            long_term_timing,
            short_term_timing,
            tracking,
            local_tracking_label,
            tags: Default::default(),
        }
    }

    pub(crate) fn response_topic(&self) -> Option<&str> {
        self.response_topic.as_deref()
    }

    pub(crate) fn set_response_topic(&mut self, response_topic: &str) {
        self.response_topic = Some(response_topic.to_owned());
    }

    pub fn tags(&self) -> &ExtraTags {
        &self.tags
    }

    pub fn set_tags(&mut self, tags: ExtraTags) -> &mut Self {
        self.tags = tags;
        self
    }
}

pub type OutgoingResponse<T> = OutgoingMessageContent<T, OutgoingResponseProperties>;

impl<T> OutgoingResponse<T>
where
    T: serde::Serialize,
{
    /// Builds a unicast response to publish.
    ///
    /// # Arguments
    ///
    /// * `payload` – any serializable value.
    /// * `properties` – properties of the outgoing response.
    /// * `to` – destination [AgentId](../struct.AgentId.html).
    /// * `version` – destination agent's API version.
    ///
    /// # Example
    ///
    /// ```
    /// let short_term_timing = OutgoingShortTermTimingProperties::until_now(start_timestamp);
    /// let props = request.properties().to_response(ResponseStatus::OK, short_term_timing)
    /// let to = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let message = OutgoingResponse::unicast(json!({ "foo": "bar" }), props, to, "v1");
    /// ```
    pub fn unicast<A>(
        payload: T,
        properties: OutgoingResponseProperties,
        to: &A,
        version: &str,
    ) -> OutgoingMessage<T>
    where
        A: Addressable,
    {
        OutgoingMessage::Response(Self::new(
            payload,
            properties,
            Destination::Unicast(to.as_agent_id().to_owned(), version.to_owned()),
        ))
    }
}

impl<T: serde::Serialize> Publishable for OutgoingResponse<T> {
    fn destination_topic(&self, publisher: &Address) -> Result<String, Error> {
        match self.properties().response_topic() {
            Some(response_topic) => Ok(response_topic.to_owned()),
            None => match self.destination {
                Destination::Unicast(ref agent_id, ref version) => Ok(format!(
                    "agents/{agent_id}/api/{version}/in/{app}",
                    agent_id = agent_id,
                    version = version,
                    app = publisher.id().as_account_id(),
                )),
                _ => Err(Error::new(&format!(
                    "destination = '{:?}' is incompatible with response message type",
                    self.destination,
                ))),
            },
        }
    }

    fn qos(&self) -> QoS {
        QoS::AtLeastOnce
    }
}
