use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::serde::{
    duration_milliseconds_string_option, ts_milliseconds_string, ts_milliseconds_string_option,
};

/// Timing properties that persist through a message chain.
///
/// See [OutgoingShortTermTimingProperties](OutgoingShortTermTimingProperties.html) for more explanation
/// on timings.
///
/// There are two kinds of properties: regular an cumulative.
/// Regular properties just get proxied without change to the next message in the chain.
/// Cumulative properties sum corresponding values from
/// [OutgoingShortTermTimingProperties](OutgoingShortTermTimingProperties.html).
///
/// If you use methods like [to_response](type.IncomingRequest.html#method.to_response),
/// [to_request](struct.IncomingRequestProperties.html#method.to_request),
/// [to_event](struct.IncomingEventProperties.html#method.to_event) and similar then
/// you shouldn't think about long term timings since they already take care of all these things.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LongTermTimingProperties {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "duration_milliseconds_string_option"
    )]
    local_initial_timediff: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "ts_milliseconds_string_option"
    )]
    initial_timestamp: Option<DateTime<Utc>>,
    #[serde(with = "ts_milliseconds_string")]
    broker_timestamp: DateTime<Utc>,
    #[serde(with = "ts_milliseconds_string")]
    broker_processing_timestamp: DateTime<Utc>,
    #[serde(with = "ts_milliseconds_string")]
    broker_initial_processing_timestamp: DateTime<Utc>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "duration_milliseconds_string_option"
    )]
    cumulative_authorization_time: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "duration_milliseconds_string_option"
    )]
    cumulative_processing_time: Option<Duration>,
}

impl LongTermTimingProperties {
    /// Updates cumulative values with the given
    /// [OutgoingShortTermTimingProperties](struct.OutgoingShortTermTimingProperties.html) values.
    ///
    /// Prefer using [to_response](type.IncomingRequest.html#method.to_response) and similar
    /// methods for building responses. If you by any chance can't use them but still want
    /// to pass [LongTermTimingProperties](struct.LongTermTimingProperties.html) manually
    /// then this is the method to call to keep timings consistent.
    ///
    /// # Arguments
    ///
    /// * `short_timing` – a reference to
    /// [OutgoingShortTermTimingProperties](struct.OutgoingShortTermTimingProperties.html) object with
    /// values to increase long term timings with.
    ///
    /// # Example
    ///
    /// ```
    /// let short_term_timing = OutgoingShortTermTimingProperties::until_now(start_timestamp);
    ///
    /// let long_term_timing = response
    ///     .properties()
    ///     .long_term_timing()
    ///     .clone()
    ///     .update_cumulative_timings(&short_term_timing);
    ///
    /// let props = OutgoingResponseProperties::new(
    ///     response.properties().status(),
    ///     request.correlation_data(),
    ///     long_term_timing,
    ///     short_term_timing,
    ///     response.properties().tracking().clone(),
    /// );
    ///
    /// let message = OutgoingResponse::unicast(
    ///     response.payload().to_owned(),
    ///     props,
    ///     request.properties(),
    ///     "v1"
    /// );
    /// ```
    pub fn update_cumulative_timings(
        self,
        short_timing: &OutgoingShortTermTimingProperties,
    ) -> Self {
        let cumulative_authorization_time = short_timing
            .authorization_time
            .map(|increment| {
                self.cumulative_authorization_time
                    .map_or(increment, |initial| initial + increment)
            })
            .or(self.cumulative_authorization_time);

        let cumulative_processing_time = short_timing
            .processing_time
            .map(|increment| {
                self.cumulative_processing_time
                    .map_or(increment, |initial| initial + increment)
            })
            .or(self.cumulative_processing_time);

        Self {
            cumulative_authorization_time,
            cumulative_processing_time,
            ..self
        }
    }
}

/// Timing properties of a single message in a chain.
///
/// Consider a service that receives a request and in part makes a request to another service.
/// The second service sends the response and then first services sends the response to the client.
/// Here we have a chain of four messages: request -> request -> response -> response.
///
/// For monitoring and analytical purposes it's useful to know how long it takes as a whole
/// and which part of the system make the biggest latency.
///
/// The conventions contain a number of properties that messages must contain.
///
/// For API simplicity in svc-agent they are separated in two structs.
/// Those which gets passed through the whole chain are
/// [LongTermTimingProperties](LongTermTimingProperties.html) and those which are related
/// only to a single message in the chain are in this struct.
///
/// When starting processing a request you should save the current time and when it's finished
/// you should call `until_now`(#method.until_now) function with this value and then pass the
/// result object to [OutgoingMessageProperties](struct.OutgoingMessageProperties.html).
///
/// If you make an authorization call to an external system during the processing you may want to
/// measure it during the call and set it to the object to monitor authorization latency as well.
///
/// # Example
///
/// ```
/// let start_timestamp = Utc::now();
/// let authz_time = authorize(&request)?;
/// let response_payload = process_request(&request)?;
///
/// let mut short_term_timing = OutgoingShortTermTimingProperties::until_now(start_timestamp);
/// short_term_timing.set_authorization_time(authz_time);
///
/// request.to_response(response_payload, ResponseStatus::OK, short_term_timing, "v1")
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutgoingShortTermTimingProperties {
    #[serde(with = "ts_milliseconds_string")]
    timestamp: DateTime<Utc>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "duration_milliseconds_string_option"
    )]
    processing_time: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "duration_milliseconds_string_option"
    )]
    authorization_time: Option<Duration>,
}

impl OutgoingShortTermTimingProperties {
    /// Builds [OutgoingShortTermTimingProperties](OutgoingShortTermTimingProperties.html) and sets
    /// processing time in one call.
    ///
    /// # Arguments
    ///
    /// * `start_timestamp` – UTC timestamp of message processing beginning.
    ///
    /// # Example
    ///
    /// ```
    /// let mut short_term_timing = OutgoingShortTermTimingProperties::until_now(start_timestamp);
    /// ```
    pub fn until_now(start_timestamp: DateTime<Utc>) -> Self {
        let now = Utc::now();
        let mut timing = Self::new(now);
        timing.set_processing_time(now - start_timestamp);
        timing
    }

    /// Builds [OutgoingShortTermTimingProperties](OutgoingShortTermTimingProperties.html)
    /// by explicit timestamp.
    ///
    /// # Arguments
    ///
    /// `timestamp` – UTC timestamp of message processing finish.
    ///
    /// # Example
    ///
    /// ```
    /// let mut short_term_timing = OutgoingShortTermTimingProperties::until_now(Utc::now());
    /// ```
    pub fn new(timestamp: DateTime<Utc>) -> Self {
        Self {
            timestamp,
            processing_time: None,
            authorization_time: None,
        }
    }

    pub fn set_processing_time(&mut self, processing_time: Duration) -> &mut Self {
        self.processing_time = Some(processing_time);
        self
    }

    pub fn set_authorization_time(&mut self, authorization_time: Duration) -> &mut Self {
        self.authorization_time = Some(authorization_time);
        self
    }
}

pub type ShortTermTimingProperties = OutgoingShortTermTimingProperties;

/// This is similar to [OutgoingShortTermTimingProperties](OutgoingShortTermTimingProperties.html)
/// but for incoming messages. The only difference is more loose optionality restrictions.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IncomingShortTermTimingProperties {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "ts_milliseconds_string_option"
    )]
    timestamp: Option<DateTime<Utc>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "duration_milliseconds_string_option"
    )]
    processing_time: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "duration_milliseconds_string_option"
    )]
    authorization_time: Option<Duration>,
}
