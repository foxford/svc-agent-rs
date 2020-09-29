use serde_derive::{Deserialize, Serialize};

use super::super::*;
use crate::{AccountId, Addressable, AgentId, Authenticable};

/// Properties of an incoming request.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncomingRequestProperties {
    method: String,
    correlation_data: String,
    response_topic: String,
    #[serde(flatten)]
    conn: ConnectionProperties,
    broker_agent_id: AgentId,
    #[serde(flatten)]
    long_term_timing: LongTermTimingProperties,
    #[serde(flatten)]
    short_term_timing: IncomingShortTermTimingProperties,
    #[serde(flatten)]
    tracking: TrackingProperties,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_tracking_label: Option<String>,
}

impl IncomingRequestProperties {
    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
    }

    pub fn response_topic(&self) -> &str {
        &self.response_topic
    }

    pub fn broker_agent_id(&self) -> &AgentId {
        &self.broker_agent_id
    }

    pub fn long_term_timing(&self) -> &LongTermTimingProperties {
        &self.long_term_timing
    }

    pub fn short_term_timing(&self) -> &IncomingShortTermTimingProperties {
        &self.short_term_timing
    }

    pub fn tracking(&self) -> &TrackingProperties {
        &self.tracking
    }

    pub fn local_tracking_label(&self) -> &Option<String> {
        &self.local_tracking_label
    }

    pub fn to_connection(&self) -> Connection {
        self.conn.to_connection()
    }

    /// Builds [OutgoingEventProperties](struct.OutgoingEventProperties.html) based on the
    /// [IncomingRequestProperties](struct.IncomingRequestProperties.html).
    ///
    /// Use it to publish an event when something worth notifying subscribers happens during
    /// the request processing.
    ///
    /// # Arguments
    ///
    /// * `label` – outgoing event label.
    /// * `short_term_timing` – outgoing event's short term timing properties.
    ///
    /// # Example
    ///
    /// ```
    /// let short_term_timing = OutgoingShortTermTimingProperties::until_now(start_timestamp);
    /// let out_props = in_props.to_event("agent.enter", short_term_timing);
    /// ```
    pub fn to_event(
        &self,
        label: &'static str,
        short_term_timing: OutgoingShortTermTimingProperties,
    ) -> OutgoingEventProperties {
        let long_term_timing = self.update_long_term_timing(&short_term_timing);
        let mut props = OutgoingEventProperties::new(label, short_term_timing);
        props.set_long_term_timing(long_term_timing);
        props.set_tracking(self.tracking.clone());
        if let Some(ref label) = self.local_tracking_label {
            props.set_local_tracking_label(label.to_owned());
        }
        props
    }

    /// Builds [OutgoingRequestProperties](struct.OutgoingRequestProperties.html) based on the
    /// [IncomingRequestProperties](struct.IncomingRequestProperties.html).
    ///
    /// Use it to send a request to another service while handling a request.
    ///
    /// # Arguments
    ///
    /// * `method` – request method.
    /// * `response_topic` – topic for response.
    /// * `correlation_data` – any string to correlate request with response.
    /// * `short_term_timing` – outgoing request's short term timing properties.
    ///
    /// # Example
    ///
    /// ```
    /// let out_props = in_props.to_request(
    ///     "room.enter",
    ///     &Subscription::unicast_responses(),
    ///     OutgoingShortTermTimingProperties::until_now(start_timestamp),
    /// );
    /// ```
    pub fn to_request(
        &self,
        method: &str,
        response_topic: &str,
        correlation_data: &str,
        short_term_timing: OutgoingShortTermTimingProperties,
    ) -> OutgoingRequestProperties {
        let long_term_timing = self.update_long_term_timing(&short_term_timing);

        let mut props = OutgoingRequestProperties::new(
            method,
            response_topic,
            correlation_data,
            short_term_timing,
        );

        props.set_long_term_timing(long_term_timing);
        props.set_tracking(self.tracking.clone());
        if let Some(ref label) = self.local_tracking_label {
            props.set_local_tracking_label(label.to_owned());
        }
        props
    }

    /// Builds [OutgoingResponseProperties](struct.OutgoingResponseProperties.html) based on
    /// the [IncomingRequestProperties](struct.IncomingRequestProperties.html).
    ///
    /// Use it to response on a request.
    ///
    /// # Arguments
    ///
    /// * `status` – response status.
    /// * `short_term_timing` – outgoing response's short term timings properties.
    ///
    /// # Example
    ///
    /// ```
    /// let short_term_timing = OutgoingShortTermTimingProperties::until_now(start_timestamp);
    /// let out_props = in_props.to_response(ResponseStatus::OK, short_term_timing);
    /// ```
    pub fn to_response(
        &self,
        status: ResponseStatus,
        short_term_timing: OutgoingShortTermTimingProperties,
    ) -> OutgoingResponseProperties {
        let mut props = OutgoingResponseProperties::new(
            status,
            &self.correlation_data,
            self.update_long_term_timing(&short_term_timing),
            short_term_timing,
            self.tracking.clone(),
            self.local_tracking_label.clone(),
        );

        props.set_response_topic(&self.response_topic);
        props
    }

    fn update_long_term_timing(
        &self,
        short_term_timing: &OutgoingShortTermTimingProperties,
    ) -> LongTermTimingProperties {
        self.long_term_timing
            .clone()
            .update_cumulative_timings(short_term_timing)
    }
}

impl Authenticable for IncomingRequestProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.conn.as_account_id()
    }
}

impl Addressable for IncomingRequestProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.conn.as_agent_id()
    }
}

pub type IncomingRequest<T> = IncomingMessageContent<T, IncomingRequestProperties>;

impl<T> IncomingRequest<T> {
    /// Builds [OutgoingResponse](OutgoingResponse.html) based on
    /// the [IncomingRequest](IncomingRequest.html).
    ///
    /// Use it to response on a request.
    ///
    /// # Arguments
    ///
    /// * `data` – serializable response payload.
    /// * `status` – response status.
    /// * `timing` – outgoing response's short term timing properties.
    ///
    /// # Example
    ///
    /// ```
    /// let response = request.to_response(
    ///     json!({ "foo": "bar" }),
    ///     ResponseStatus::OK,
    ///     OutgoingShortTermTimingProperties::until_now(start_timestamp),
    /// );
    /// ```
    pub fn to_response<R>(
        &self,
        data: R,
        status: ResponseStatus,
        timing: OutgoingShortTermTimingProperties,
        api_version: &str,
    ) -> OutgoingMessage<R>
    where
        R: serde::Serialize,
    {
        OutgoingMessage::Response(OutgoingResponse::new(
            data,
            self.properties().to_response(status, timing),
            Destination::Unicast(
                self.properties().as_agent_id().clone(),
                api_version.to_owned(),
            ),
        ))
    }
}

impl<String: std::ops::Deref<Target = str>> IncomingRequest<String> {
    pub fn convert_payload<T>(message: &IncomingRequest<String>) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = serde_json::from_str::<T>(&message.payload()).map_err(|e| {
            Error::new(&format!(
                "error deserializing payload of an envelope, {}",
                &e
            ))
        })?;
        Ok(payload)
    }

    pub fn convert<T>(message: IncomingRequest<String>) -> Result<IncomingRequest<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let props = message.properties().to_owned();
        let payload = serde_json::from_str::<T>(&message.payload()).map_err(|e| {
            Error::new(&format!(
                "error deserializing payload of an envelope, {}",
                &e
            ))
        })?;
        Ok(IncomingRequest::new(payload, props))
    }
}
