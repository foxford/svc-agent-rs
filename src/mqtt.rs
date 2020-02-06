use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    serde::{
        duration_milliseconds_string_option, session_ids_list, ts_milliseconds_string,
        ts_milliseconds_string_option,
    },
    AccountId, Addressable, AgentId, Authenticable, Destination, Error, EventSubscription,
    RequestSubscription, ResponseSubscription, SharedGroup, Source,
};

////////////////////////////////////////////////////////////////////////////////

/// Agent configuration.
///
/// # Options
///
/// * `uri` – MQTT broker URI (required).
/// * `clean_session` – whether to start a clean sessinon or continue the persisted session.
/// Default: `true`.
/// * `keep_alive_interval` – keep alive time to ping the broker. Default: 30 sec.
/// * `reconnect_interval` – reconnection attempts interval. Default: 10 sec.
/// * `outgoing_message_queue_size` – maximum messages in-flight. Default: 100.
/// * `incomming_message_queue_size` – notification channel capacity. Default: 10.
/// * `password` – MQTT broker password.
#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    uri: String,
    clean_session: Option<bool>,
    keep_alive_interval: Option<u64>,
    reconnect_interval: Option<u64>,
    outgoing_message_queue_size: Option<usize>,
    incomming_message_queue_size: Option<usize>,
    password: Option<String>,
}

impl AgentConfig {
    /// Sets `password` field to the config.
    ///
    /// Use if you don't store the password in the config file but in an environment variable,
    /// somewhere else or generate an access token in runtime.
    pub fn set_password(&mut self, value: &str) -> &mut Self {
        self.password = Some(value.to_owned());
        self
    }
}

/// An agent builder.
#[derive(Debug)]
pub struct AgentBuilder {
    connection: Connection,
    api_version: String,
}

impl AgentBuilder {
    /// Creates a new [AgentBuilder](struct.AgentBuilder.html).
    ///
    /// # Arguments
    ///
    /// * `agent_id` – [AgentId](../struct.AgentId.html) to connect as.
    /// * `api_version` – agent's API version string.
    ///
    /// # Example
    ///
    /// ```
    /// ket account_id = AccountId::new("service_name", "svc.example.org");
    /// let agent_id = AgentId::new("instance01", account_id);
    /// let builder = AgentBuilder::new(agent_id, "v1");
    /// ```
    pub fn new(agent_id: AgentId, api_version: &str) -> Self {
        Self {
            connection: Connection::new(agent_id),
            api_version: api_version.to_owned(),
        }
    }

    /// Sets a connection version.
    ///
    /// This is different from agent's API version.
    /// Connection version is the version of conventions that this library implements.
    /// Currently it's `v2` but if you know what you're doing you may override it.
    pub fn connection_version(self, version: &str) -> Self {
        let mut connection = self.connection;
        connection.set_version(version);
        Self { connection, ..self }
    }

    /// Sets a connection mode for the agent to claim.
    ///
    /// Connection mode defines a level a privileges an agent may use.
    /// See [ConnectionMode](enum.ConnectionMode.html) for available modes and details.
    ///
    /// The broker requires authorization to use the claimed mode and may refuse the connection
    /// if not authorized.
    pub fn connection_mode(self, mode: ConnectionMode) -> Self {
        let mut connection = self.connection;
        connection.set_mode(mode);
        Self { connection, ..self }
    }

    /// Starts an MQTT client and in case of successfull connection returns a tuple containing
    /// an [Agent](struct.Agent.html) instance and a channel receiver which one can
    /// iterate over to get incoming messages.
    ///
    /// # Example
    ///
    /// ```
    /// let (agent, rx) = builder.start(&config)?;
    ///
    /// // Subscribe to requests.
    /// agent.subscribe(
    ///     &Subscription::multicast_requests(Some("v1")),
    ///     QoS::AtMostOnce,
    ///     Some(&group),
    /// )?;
    ///
    /// // Message handling loop.
    /// for notification in rx {
    ///     match notification {
    ///         svc_agent::mqtt::Notification::Publish(message) => {
    ///             println!(
    ///                 "Incoming message: {} to topic {}",
    ///                 message.payload.as_slice(),
    ///                 message.topic_name
    ///             );
    ///         }
    ///         _ => ()
    ///     }
    /// }
    /// ```
    pub fn start(
        self,
        config: &AgentConfig,
    ) -> Result<(Agent, rumqtt::Receiver<rumqtt::Notification>), Error> {
        let options = Self::mqtt_options(&self.connection, &config)?;
        let (tx, rx) = rumqtt::MqttClient::start(options)
            .map_err(|e| Error::new(&format!("error starting MQTT client, {}", e)))?;

        let agent = Agent::new(self.connection.agent_id, &self.api_version, tx);
        Ok((agent, rx))
    }

    fn mqtt_options(
        connection: &Connection,
        config: &AgentConfig,
    ) -> Result<rumqtt::MqttOptions, Error> {
        let uri = config
            .uri
            .parse::<http::Uri>()
            .map_err(|e| Error::new(&format!("error parsing MQTT connection URL, {}", e)))?;
        let host = uri.host().ok_or_else(|| Error::new("missing MQTT host"))?;
        let port = uri
            .port_part()
            .ok_or_else(|| Error::new("missing MQTT port"))?;

        // For MQTT 3 we specify connection version and mode in username field
        // because it doesn't have user properties like MQTT 5.
        let username = format!("{}::{}", connection.version, connection.mode);

        let password = config
            .password
            .to_owned()
            .unwrap_or_else(|| String::from(""));

        // TODO: change to convenient code as soon as the PR will be merged
        // https://github.com/AtherEnergy/rumqtt/pull/145
        let mut opts =
            rumqtt::MqttOptions::new(connection.agent_id.to_string(), host, port.as_u16());
        opts = match config.clean_session {
            Some(value) => opts.set_clean_session(value),
            _ => opts,
        };
        opts = match config.keep_alive_interval {
            Some(value) => opts.set_keep_alive(value as u16),
            _ => opts,
        };
        opts = match config.reconnect_interval {
            Some(value) => opts.set_reconnect_opts(rumqtt::ReconnectOptions::Always(value)),
            _ => opts,
        };
        opts = match config.incomming_message_queue_size {
            Some(value) => opts.set_notification_channel_capacity(value),
            _ => opts,
        };
        opts = match config.outgoing_message_queue_size {
            Some(value) => opts.set_inflight(value),
            _ => opts,
        };
        opts = opts.set_security_opts(rumqtt::SecurityOptions::UsernamePassword(
            username, password,
        ));

        Ok(opts)
    }
}

#[derive(Clone, Debug)]
pub struct Address {
    id: AgentId,
    version: String,
}

impl Address {
    pub fn new(id: AgentId, version: &str) -> Self {
        Self {
            id,
            version: version.to_owned(),
        }
    }

    pub fn id(&self) -> &AgentId {
        &self.id
    }

    pub fn version(&self) -> &str {
        &self.version
    }
}

#[derive(Clone)]
pub struct Agent {
    address: Address,
    tx: rumqtt::MqttClient,
}

impl Agent {
    fn new(id: AgentId, api_version: &str, tx: rumqtt::MqttClient) -> Self {
        Self {
            address: Address::new(id, api_version),
            tx,
        }
    }

    pub fn address(&self) -> &Address {
        &self.address
    }

    pub fn id(&self) -> &AgentId {
        &self.address.id()
    }

    /// Publish a message.
    ///
    /// This method is a shorthand to dump and publish the message with a single call.
    /// If you want to print out the dump before or after publishing or assert it in tests
    /// consider using [IntoPublishableDump::into_dump](trait.IntoPublishableDump.html#method.into_dump)
    /// and [publish_dump](#method.publish_dump).
    ///
    /// # Arguments
    ///
    /// * `message` – a boxed message of any type implementing
    /// [Publishable](trait.Publishable.html) trait.
    ///
    /// # Example
    ///
    /// ```
    /// let props = OutgoingRequestProperties::new(
    ///     "system.ping",
    ///     Subscription::unicast_responses_from(to).subscription_topic(agent.id(), "v1")?,
    ///     "random-string-123",
    ///     ShortTermTimingProperties::new(Utc::now()),
    /// );
    ///
    /// let message = OutgoingMessage::new(
    ///     json!({ "ping": "hello" }),
    ///     props,
    ///     Destination::Unicast(agent.id().clone(), "v1"),
    /// );
    ///
    /// agent.publish(Box::new(message))?;
    /// ```
    pub fn publish(&mut self, message: Box<dyn IntoPublishableDump>) -> Result<(), Error> {
        let dump = message.into_dump(&self.address)?;
        self.publish_dump(dump)
    }

    /// Publish a dumped message.
    ///
    /// # Arguments
    ///
    /// * `dump` – message dump.
    ///
    /// # Example
    ///
    /// ```
    /// let props = OutgoingRequestProperties::new(
    ///     "system.ping",
    ///     Subscription::unicast_responses_from(to).subscription_topic(agent.id(), "v1")?,
    ///     "random-string-123",
    ///     ShortTermTimingProperties::new(Utc::now()),
    /// );
    ///
    /// let message = OutgoingMessage::new(
    ///     json!({ "ping": "hello" }),
    ///     props,
    ///     Destination::Unicast(agent.id().clone(), "v1"),
    /// );
    ///
    /// let dump = Box::new(message).into_dump();
    /// agent.publish_dump(dump.clone())?;
    /// println!("Message published: {}", dump);
    /// ```
    pub fn publish_dump(&mut self, dump: PublishableDump) -> Result<(), Error> {
        self.tx
            .publish(dump.topic, dump.qos, false, dump.payload)
            .map_err(|e| Error::new(&format!("error publishing MQTT message, {}", &e)))
    }

    /// Subscribe to a topic.
    ///
    /// # Arguments
    ///
    /// * `subscription` – the [Subscription](struct.Subscription.html).
    /// * `qos` – quality of service. See [QoS](enum.QoS.html) for available values.
    /// * `maybe_group` – [SharedGroup](struct.SharedGroup.html) in case of multicast subscription.
    ///
    /// # Example
    ///
    /// ```
    /// agent.subscribe(
    ///     &Subscription::multicast_requests(Some("v1")),
    ///     QoS::AtMostOnce,
    ///     Some(&group),
    /// )?;
    /// ```
    pub fn subscribe<S>(
        &mut self,
        subscription: &S,
        qos: QoS,
        maybe_group: Option<&SharedGroup>,
    ) -> Result<(), Error>
    where
        S: SubscriptionTopic,
    {
        let mut topic = subscription.subscription_topic(self.id(), self.address.version())?;
        if let Some(ref group) = maybe_group {
            topic = format!("$share/{group}/{topic}", group = group, topic = topic);
        };

        self.tx
            .subscribe(topic, qos)
            .map_err(|e| Error::new(&format!("error creating MQTT subscription, {}", e)))?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Connection mode of an agent that defines the level of privileges.
#[derive(Debug, Clone)]
pub enum ConnectionMode {
    /// This mode locks the agent in his home topic allowing to publish and subscribe only to
    /// topics that start with `agent/AGENT_ID/api/`.
    ///
    /// It must be used by end user agents.
    Default,
    /// This mode allows the agent to publish to any topic.
    /// It enables the service to send responses to end users and other services.
    ///
    /// It mode must be used by regular service agents.
    Service,
    /// This mode allows also subscribing to any topic.
    ///
    /// It shouldn't generally be used at all in production environment but may be useful for
    /// debugging and administrating.
    Observer,
    /// This mode allows publishing messages on behalf of another agent.
    ///
    /// It's intended for bridge service only that enable interaction with the system through
    /// protocols different from MQTT.
    Bridge,
}

impl fmt::Display for ConnectionMode {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}",
            match self {
                ConnectionMode::Default => "default",
                ConnectionMode::Service => "service",
                ConnectionMode::Observer => "observer",
                ConnectionMode::Bridge => "bridge",
            }
        )
    }
}

impl FromStr for ConnectionMode {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "default" => Ok(ConnectionMode::Default),
            "service" => Ok(ConnectionMode::Service),
            "observer" => Ok(ConnectionMode::Observer),
            "bridge" => Ok(ConnectionMode::Bridge),
            _ => Err(Error::new(&format!(
                "invalid value for the connection mode: {}",
                val
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Connection {
    agent_id: AgentId,
    version: String,
    mode: ConnectionMode,
}

impl Connection {
    fn new(agent_id: AgentId) -> Self {
        Self {
            agent_id,
            version: String::from("v2"),
            mode: ConnectionMode::Default,
        }
    }

    fn set_version(&mut self, value: &str) -> &mut Self {
        self.version = value.to_owned();
        self
    }

    fn set_mode(&mut self, value: ConnectionMode) -> &mut Self {
        self.mode = value;
        self
    }

    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn mode(&self) -> &ConnectionMode {
        &self.mode
    }
}

impl fmt::Display for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}/{}/{}", self.version, self.mode, self.agent_id,)
    }
}

impl FromStr for Connection {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val.split('/').collect::<Vec<&str>>().as_slice() {
            [version_str, mode_str, agent_id_str] => {
                let version = version_str.to_string();
                let mode = ConnectionMode::from_str(mode_str)?;
                let agent_id = AgentId::from_str(agent_id_str)?;
                Ok(Self {
                    version,
                    mode,
                    agent_id,
                })
            }
            _ => Err(Error::new(&format!(
                "invalid value for connection: {}",
                val
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConnectionProperties {
    agent_id: AgentId,
    #[serde(rename = "connection_version")]
    version: String,
    #[serde(rename = "connection_mode")]
    mode: ConnectionMode,
}

impl ConnectionProperties {
    fn to_connection(&self) -> Connection {
        let mut connection = Connection::new(self.agent_id.clone());
        connection.set_version(&self.version);
        connection.set_mode(self.mode.clone());
        connection
    }
}

impl Authenticable for ConnectionProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.agent_id.as_account_id()
    }
}

impl Addressable for ConnectionProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.agent_id
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Timing properties that persist through a message chain.
///
/// See [ShortTermTimingProperties](ShortTermTimingProperties.html) for more explanation
/// on timings.
///
/// There are two kinds of properties: regular an cumulative.
/// Regular properties just get proxied without change to the next message in the chain.
/// Cumulative properties sum corresponding values from
/// [ShortTermTimingProperties](ShortTermTimingProperties.html).
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
    /// [ShortTermTimingProperties](struct.ShortTermTimingProperties.html) values.
    ///
    /// Prefer using [to_response](type.IncomingRequest.html#method.to_response) and similar
    /// methods for building responses. If you by any chance can't use them but still want
    /// to pass [LongTermTimingProperties](struct.LongTermTimingProperties.html) manually
    /// then this is the method to call to keep timings consistent.
    ///
    /// # Arguments
    ///
    /// * `short_timing` – a reference to
    /// [ShortTermTimingProperties](struct.ShortTermTimingProperties.html) object with
    /// values to increase long term timings with.
    ///
    /// # Example
    ///
    /// ```
    /// let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
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
    pub fn update_cumulative_timings(self, short_timing: &ShortTermTimingProperties) -> Self {
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
/// meausure it during the call and set it to the object to monitor authorization latency as well.
///
/// # Example
///
/// ```
/// let start_timestamp = Utc::now();
/// let authz_time = authorize(&request)?;
/// let response_payload = process_request(&request)?;
///
/// let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
/// short_term_timing.set_authorization_time(authz_time);
///
/// request.to_response(response_payload, ResponeStatus::OK, short_term_timing, "v1")
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ShortTermTimingProperties {
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

impl ShortTermTimingProperties {
    /// Builds [ShortTermTimingProperties](ShortTermTimingProperties.html) and sets
    /// processing time in one call.
    ///
    /// # Arguments
    ///
    /// * `start_timestamp` – UTC timestamp of message processing beginning.
    ///
    /// # Example
    ///
    /// ```
    /// let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    /// ```
    pub fn until_now(start_timestamp: DateTime<Utc>) -> Self {
        let now = Utc::now();
        let mut timing = Self::new(now);
        timing.set_processing_time(now - start_timestamp);
        timing
    }

    /// Builds [ShortTermTimingProperties](ShortTermTimingProperties.html)
    /// by explicit timestamp.
    ///
    /// # Arguments
    ///
    /// `timestamp` – UTC timestamp of message processing finish.
    ///
    /// # Example
    ///
    /// ```
    /// let mut short_term_timing = ShortTermTimingProperties::until_now(Utc::now());
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

///////////////////////////////////////////////////////////////////////////////

/// Tracking session ID.
#[derive(Clone, Debug)]
pub struct SessionId {
    agent_session_label: Uuid,
    broker_session_label: Uuid,
}

impl FromStr for SessionId {
    type Err = Error;

    /// Parses a [SessionId](struct.SessionId.html) from a string of two UUIDs
    /// separated by a dot.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let components = s.splitn(2, ".").collect::<Vec<&str>>();

        match components[..] {
            [agent_session_label_str, broker_session_label_str] => {
                let agent_session_label =
                    Uuid::parse_str(agent_session_label_str).map_err(|err| {
                        let msg = format!("Failed to parse agent session label UUID: {}", err);
                        Error::new(&msg)
                    })?;

                let broker_session_label =
                    Uuid::parse_str(broker_session_label_str).map_err(|err| {
                        let msg = format!("Failed to parse broker session label UUID: {}", err);
                        Error::new(&msg)
                    })?;

                Ok(Self {
                    agent_session_label,
                    broker_session_label,
                })
            }
            _ => Err(Error::new(
                "Failed to parse SessionId. Expected 2 UUIDs separated by .",
            )),
        }
    }
}

impl fmt::Display for SessionId {
    /// Dumps a [SessionId](struct.SessionId.html) to a string of two UUIDs separated by a dot.
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}.{}",
            self.agent_session_label, self.broker_session_label
        )
    }
}

/// Message chain ID.
#[derive(Clone, Debug)]
pub struct TrackingId {
    label: Uuid,
    session_id: SessionId,
}

impl FromStr for TrackingId {
    type Err = Error;

    /// Parses a [TrackingId](struct.TrackingId.html) from a string of three UUIDs
    /// separated by a dot.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let components = s.splitn(2, ".").collect::<Vec<&str>>();

        match components[..] {
            [label_str, session_id_str] => {
                let label = Uuid::parse_str(label_str).map_err(|err| {
                    let msg = format!("Failed to parse tracking id label UUID: {}", err);
                    Error::new(&msg)
                })?;

                Ok(Self {
                    label,
                    session_id: SessionId::from_str(session_id_str)?,
                })
            }
            _ => Err(Error::new(
                "Failed to parse TrackingId. Expected 3 UUIDs separated by .",
            )),
        }
    }
}

impl fmt::Display for TrackingId {
    /// Dumps a [TrackingId](struct.TrackingId.html) to a string of three UUIDs separated by a
    /// dot.
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}.{}", self.label, self.session_id)
    }
}

/// Message tracking properties.
///
/// Apart from [LongTermTimingProperties](struct.LongTermTimingProperties.html) and
/// [ShortTermTimingProperties](struct.ShortTermTimingProperties.html) there are also
/// tracking properties. They get assigned by the broker but since the decision on whether to
/// continue the chain by the next message either start a new chain is up to the agent,
/// tracking properties needs to be proxied in the former case.
///
/// Proxying is performed by [to_response](type.IncomingRequest.html#method.to_response) and
/// the like methods.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackingProperties {
    tracking_id: TrackingId,
    #[serde(with = "session_ids_list")]
    session_tracking_label: Vec<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_tracking_label: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////

/// Properties of an incoming event message.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncomingEventProperties {
    #[serde(flatten)]
    conn: ConnectionProperties,
    label: Option<String>,
    #[serde(flatten)]
    long_term_timing: LongTermTimingProperties,
    #[serde(flatten)]
    tracking: TrackingProperties,
}

impl IncomingEventProperties {
    pub fn to_connection(&self) -> Connection {
        self.conn.to_connection()
    }

    pub fn label(&self) -> Option<&str> {
        self.label.as_ref().map(|l| &**l)
    }

    pub fn tracking(&self) -> &TrackingProperties {
        &self.tracking
    }

    /// Builds [OutgoingEventProperties](struct.OutgoingEventProperties.html) based on the
    /// [IncomingEventProperties](struct.IncomingEventProperties.html).
    ///
    /// Use it to dispatch an event as a reaction on another event.
    ///
    /// # Arguments
    ///
    /// * `label` – outgoing event label.
    /// * `short_term_timing` – outgoing event's short term timing properties.
    ///
    /// # Example
    ///
    /// ```
    /// let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    /// let out_props = in_props.to_event("agent.enter", short_term_timing);
    /// ```
    pub fn to_event(
        &self,
        label: &'static str,
        short_term_timing: ShortTermTimingProperties,
    ) -> OutgoingEventProperties {
        let long_term_timing = self.update_long_term_timing(&short_term_timing);
        let mut props = OutgoingEventProperties::new(label, short_term_timing);
        props.set_long_term_timing(long_term_timing);
        props.set_tracking(self.tracking.clone());
        props
    }

    fn update_long_term_timing(
        &self,
        short_term_timing: &ShortTermTimingProperties,
    ) -> LongTermTimingProperties {
        self.long_term_timing
            .clone()
            .update_cumulative_timings(short_term_timing)
    }
}

impl Authenticable for IncomingEventProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.conn.as_account_id()
    }
}

impl Addressable for IncomingEventProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.conn.as_agent_id()
    }
}

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
    tracking: TrackingProperties,
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

    pub fn tracking(&self) -> &TrackingProperties {
        &self.tracking
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
    /// let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    /// let out_props = in_props.to_event("agent.enter", short_term_timing);
    /// ```
    pub fn to_event(
        &self,
        label: &'static str,
        short_term_timing: ShortTermTimingProperties,
    ) -> OutgoingEventProperties {
        let long_term_timing = self.update_long_term_timing(&short_term_timing);
        let mut props = OutgoingEventProperties::new(label, short_term_timing);
        props.set_long_term_timing(long_term_timing);
        props.set_tracking(self.tracking.clone());
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
    ///     ShortTermTimingProperties::until_now(start_timestamp),
    /// );
    /// ```
    pub fn to_request(
        &self,
        method: &str,
        response_topic: &str,
        correlation_data: &str,
        short_term_timing: ShortTermTimingProperties,
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
    /// let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    /// let out_props = in_props.to_response(ResponseStatus::OK, short_term_timing);
    /// ```
    pub fn to_response(
        &self,
        status: ResponseStatus,
        short_term_timing: ShortTermTimingProperties,
    ) -> OutgoingResponseProperties {
        let mut props = OutgoingResponseProperties::new(
            status,
            &self.correlation_data,
            self.update_long_term_timing(&short_term_timing),
            short_term_timing,
            self.tracking.clone(),
        );

        props.response_topic = Some(self.response_topic.to_owned());
        props
    }

    fn update_long_term_timing(
        &self,
        short_term_timing: &ShortTermTimingProperties,
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

/// Properties of an incoming response.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IncomingResponseProperties {
    #[serde(with = "crate::serde::HttpStatusCodeRef")]
    status: ResponseStatus,
    correlation_data: String,
    #[serde(flatten)]
    conn: ConnectionProperties,
    #[serde(flatten)]
    long_term_timing: LongTermTimingProperties,
    #[serde(flatten)]
    tracking: TrackingProperties,
}

impl IncomingResponseProperties {
    pub fn status(&self) -> ResponseStatus {
        self.status
    }

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
    }

    pub fn long_term_timing(&self) -> &LongTermTimingProperties {
        &self.long_term_timing
    }

    pub fn tracking(&self) -> &TrackingProperties {
        &self.tracking
    }

    pub fn to_connection(&self) -> Connection {
        self.conn.to_connection()
    }
}

impl Authenticable for IncomingResponseProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.conn.as_account_id()
    }
}

impl Addressable for IncomingResponseProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.conn.as_agent_id()
    }
}

////////////////////////////////////////////////////////////////////////////////

/// A generic received message.
#[derive(Debug)]
pub struct IncomingMessage<T, P>
where
    P: Addressable + serde::Serialize,
{
    payload: T,
    properties: P,
}

impl<T, P> IncomingMessage<T, P>
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

    pub fn properties(&self) -> &P {
        &self.properties
    }
}

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
    ///     ShortTermTimingProperties::until_now(start_timestamp),
    /// );
    /// ```
    pub fn to_response<R>(
        &self,
        data: R,
        status: ResponseStatus,
        timing: ShortTermTimingProperties,
        api_version: &str,
    ) -> OutgoingResponse<R>
    where
        R: serde::Serialize,
    {
        OutgoingMessage::new(
            data,
            self.properties.to_response(status, timing),
            Destination::Unicast(
                self.properties.as_agent_id().clone(),
                api_version.to_owned(),
            ),
        )
    }
}

pub type IncomingEvent<T> = IncomingMessage<T, IncomingEventProperties>;
pub type IncomingRequest<T> = IncomingMessage<T, IncomingRequestProperties>;
pub type IncomingResponse<T> = IncomingMessage<T, IncomingResponseProperties>;

////////////////////////////////////////////////////////////////////////////////

/// Properties of an outgoing event.
#[derive(Debug, Serialize)]
pub struct OutgoingEventProperties {
    label: &'static str,
    #[serde(flatten)]
    long_term_timing: Option<LongTermTimingProperties>,
    #[serde(flatten)]
    short_term_timing: ShortTermTimingProperties,
    #[serde(flatten)]
    tracking: Option<TrackingProperties>,
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
    ///     ShortTermTimingProperties::new(Utc::now()),
    /// );
    /// ```
    pub fn new(label: &'static str, short_term_timing: ShortTermTimingProperties) -> Self {
        Self {
            label,
            long_term_timing: None,
            short_term_timing,
            tracking: None,
        }
    }

    pub fn set_long_term_timing(&mut self, timing: LongTermTimingProperties) -> &mut Self {
        self.long_term_timing = Some(timing);
        self
    }

    pub fn set_tracking(&mut self, tracking: TrackingProperties) -> &mut Self {
        self.tracking = Some(tracking);
        self
    }
}

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
    short_term_timing: ShortTermTimingProperties,
    #[serde(flatten)]
    tracking: Option<TrackingProperties>,
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
    ///     ShortTermTimingProperties::new(Utc::now()),
    /// );
    /// ```
    pub fn new(
        method: &str,
        response_topic: &str,
        correlation_data: &str,
        short_term_timing: ShortTermTimingProperties,
    ) -> Self {
        Self {
            method: method.to_owned(),
            response_topic: response_topic.to_owned(),
            correlation_data: correlation_data.to_owned(),
            agent_id: None,
            long_term_timing: None,
            short_term_timing,
            tracking: None,
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

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
    }
}

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
    short_term_timing: ShortTermTimingProperties,
    #[serde(flatten)]
    tracking: TrackingProperties,
}

impl OutgoingResponseProperties {
    /// Builds [OutgoingResponseProperties](struct.OutgoingResponseProperties.html).
    ///
    /// Generally you shouldn't use this function and consider using
    /// [IncomingRequestProperties::to_response](struct.IncomingRequestProperties.html#method.to_respone)
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
    ///     ShortTermTimingProperties::new(Utc::now()),
    ///     req_props.tracking().clone(),
    /// );
    /// ```
    pub fn new(
        status: ResponseStatus,
        correlation_data: &str,
        long_term_timing: LongTermTimingProperties,
        short_term_timing: ShortTermTimingProperties,
        tracking: TrackingProperties,
    ) -> Self {
        Self {
            status,
            correlation_data: correlation_data.to_owned(),
            response_topic: None,
            long_term_timing,
            short_term_timing,
            tracking,
        }
    }

    fn response_topic(&self) -> Option<&str> {
        self.response_topic.as_ref().map(|t| &**t)
    }
}

////////////////////////////////////////////////////////////////////////////////

/// HTTP status code.
pub type ResponseStatus = http::StatusCode;

////////////////////////////////////////////////////////////////////////////////

/// A generic received message.
#[derive(Debug)]
pub struct OutgoingMessage<T, P>
where
    T: serde::Serialize,
{
    payload: T,
    properties: P,
    destination: Destination,
}

impl<T, P> OutgoingMessage<T, P>
where
    T: serde::Serialize,
{
    fn new(payload: T, properties: P, destination: Destination) -> Self {
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
    /// let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    ///
    /// let message = OutgoingEvent::broadcast(
    ///     json!({ "foo": "bar" }),
    ///     request.to_event("message.create", short_term_timing),
    ///     "rooms/123/events",
    /// );
    /// ```
    pub fn broadcast(payload: T, properties: OutgoingEventProperties, to_uri: &str) -> Self {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Broadcast(to_uri.to_owned()),
        )
    }
}

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
    /// let props = request.to_request(
    ///     "room.enter",
    ///     &Subscription::unicast_responses(),
    ///     ShortTermTimingProperties::until_now(start_timestamp),
    /// );
    ///
    /// let message = OutgoingRequest::multicast(json!({ "foo": "bar" }), props);
    /// ```
    pub fn multicast<A>(payload: T, properties: OutgoingRequestProperties, to: &A) -> Self
    where
        A: Authenticable,
    {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Multicast(to.as_account_id().clone()),
        )
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
    /// let props = request.to_request(
    ///     "room.enter",
    ///     &Subscription::unicast_responses(),
    ///     ShortTermTimingProperties::until_now(start_timestamp),
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
    ) -> Self
    where
        A: Addressable,
    {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Unicast(to.as_agent_id().clone(), version.to_owned()),
        )
    }
}

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
    /// let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
    /// let props = request.properties().to_response(ResponseStatus::OK, short_term_timing)
    /// let to = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let message = OutgoingResponse::unicast(json!({ "foo": "bar" }), props, to, "v1");
    /// ```
    pub fn unicast<A>(
        payload: T,
        properties: OutgoingResponseProperties,
        to: &A,
        version: &str,
    ) -> Self
    where
        A: Addressable,
    {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Unicast(to.as_agent_id().clone(), version.to_owned()),
        )
    }
}

pub type OutgoingEvent<T> = OutgoingMessage<T, OutgoingEventProperties>;
pub type OutgoingRequest<T> = OutgoingMessage<T, OutgoingRequestProperties>;
pub type OutgoingResponse<T> = OutgoingMessage<T, OutgoingResponseProperties>;

impl<T> compat::IntoEnvelope for OutgoingEvent<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<compat::OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)
            .map_err(|e| Error::new(&format!("error serializing payload of an envelope, {}", e)))?;
        let envelope = compat::OutgoingEnvelope::new(
            &payload,
            compat::OutgoingEnvelopeProperties::Event(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

impl<T> compat::IntoEnvelope for OutgoingRequest<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<compat::OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)
            .map_err(|e| Error::new(&format!("error serializing payload of an envelope, {}", e)))?;
        let envelope = compat::OutgoingEnvelope::new(
            &payload,
            compat::OutgoingEnvelopeProperties::Request(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

impl<T> compat::IntoEnvelope for OutgoingResponse<T>
where
    T: serde::Serialize,
{
    fn into_envelope(self) -> Result<compat::OutgoingEnvelope, Error> {
        let payload = serde_json::to_string(&self.payload)
            .map_err(|e| Error::new(&format!("error serializing payload of an envelope, {}", e)))?;
        let envelope = compat::OutgoingEnvelope::new(
            &payload,
            compat::OutgoingEnvelopeProperties::Response(self.properties),
            self.destination,
        );
        Ok(envelope)
    }
}

////////////////////////////////////////////////////////////////////////////////

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

impl<T: serde::Serialize> Publishable for OutgoingEvent<T>
where
    OutgoingEvent<T>: compat::IntoEnvelope,
{
    fn destination_topic(&self, publisher: &Address) -> Result<String, Error> {
        match self.destination {
            Destination::Broadcast(ref uri) => Ok(format!(
                "apps/{app}/api/{version}/{uri}",
                app = publisher.id().as_account_id(),
                version = publisher.version(),
                uri = uri,
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

impl<T: serde::Serialize> Publishable for OutgoingRequest<T>
where
    OutgoingRequest<T>: compat::IntoEnvelope,
{
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

impl<T: serde::Serialize> Publishable for OutgoingResponse<T>
where
    OutgoingResponse<T>: compat::IntoEnvelope,
{
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

////////////////////////////////////////////////////////////////////////////////

/// Dumped version of [Publishable](trait.Publishable.html).
#[derive(Clone, Debug)]
pub struct PublishableDump {
    topic: String,
    qos: QoS,
    payload: String,
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
}

pub trait IntoPublishableDump {
    /// Serializes the object into dump that can be directly published.
    fn into_dump(self: Box<Self>, publisher: &Address) -> Result<PublishableDump, Error>;
}

impl<T: Publishable + compat::IntoEnvelope> IntoPublishableDump for T {
    fn into_dump(self: Box<Self>, publisher: &Address) -> Result<PublishableDump, Error> {
        let topic = self.destination_topic(&publisher)?;
        let qos = self.qos();

        let payload = serde_json::to_string(&self.into_envelope()?)
            .map_err(|e| Error::new(&format!("error serializing an envelope, {}", &e)))?;

        Ok(PublishableDump {
            topic,
            qos,
            payload,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////

pub trait SubscriptionTopic {
    /// Returns a topic to subscribe to as string.
    ///
    /// # Arguments
    ///
    /// * `agent_id` – current agent's [AgentId](../struct.AgentId.html).
    /// * `me_version` – current agent's API version.
    ///
    /// # Example
    ///
    /// ```
    /// let me = AgentId::new("instance01", AccountId::new("me_name", "svc.example.org"));
    /// let agent = AgentId::new("instance01", AccountId::new("service_name", "svc.example.org"));
    /// let subscription = Subscription::broadcast_events(&agent, "v1", "rooms/123/events");
    /// let topic = subscription.subscription_topic(me, "v1")?;
    /// ```
    fn subscription_topic<A>(&self, agent_id: &A, me_version: &str) -> Result<String, Error>
    where
        A: Addressable;
}

impl SubscriptionTopic for &'static str {
    fn subscription_topic<A>(&self, _me: &A, _me_version: &str) -> Result<String, Error>
    where
        A: Addressable,
    {
        Ok(self.to_string())
    }
}

impl<'a> SubscriptionTopic for EventSubscription<'a> {
    fn subscription_topic<A>(&self, _me: &A, _me_version: &str) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Broadcast(ref from_account_id, ref version, ref uri) => Ok(format!(
                "apps/{app}/api/{version}/{uri}",
                app = from_account_id,
                version = version,
                uri = uri,
            )),
            _ => Err(Error::new(&format!(
                "source = '{:?}' is incompatible with event subscription",
                self.source,
            ))),
        }
    }
}

impl<'a> SubscriptionTopic for RequestSubscription<'a> {
    fn subscription_topic<A>(&self, me: &A, me_version: &str) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Multicast(Some(ref from_agent_id), ver) => Ok(format!(
                "agents/{agent_id}/api/{version}/out/{app}",
                agent_id = from_agent_id,
                version = ver.unwrap_or("+"),
                app = me.as_account_id(),
            )),
            Source::Multicast(None, ver) => Ok(format!(
                "agents/+/api/{version}/out/{app}",
                version = ver.unwrap_or("+"),
                app = me.as_account_id(),
            )),
            Source::Unicast(Some(ref from_account_id)) => Ok(format!(
                "agents/{agent_id}/api/{version}/in/{app}",
                agent_id = me.as_agent_id(),
                version = me_version,
                app = from_account_id,
            )),
            Source::Unicast(None) => Ok(format!(
                "agents/{agent_id}/api/{version}/in/+",
                agent_id = me.as_agent_id(),
                version = me_version,
            )),
            _ => Err(Error::new(&format!(
                "source = '{:?}' is incompatible with request subscription",
                self.source,
            ))),
        }
    }
}

impl<'a> SubscriptionTopic for ResponseSubscription<'a> {
    fn subscription_topic<A>(&self, me: &A, me_version: &str) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Unicast(Some(ref from_account_id)) => Ok(format!(
                "agents/{agent_id}/api/{version}/in/{app}",
                agent_id = me.as_agent_id(),
                version = me_version,
                app = from_account_id,
            )),
            Source::Unicast(None) => Ok(format!(
                "agents/{agent_id}/api/{version}/in/+",
                agent_id = me.as_agent_id(),
                version = me_version,
            )),
            _ => Err(Error::new(&format!(
                "source = '{:?}' is incompatible with response subscription",
                self.source,
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

/// MQTT 3.1 compatibility utilities.
///
/// [mqtt-gateway](https://github.com/netology-group/mqtt-gateway) supports both MQTT 3.1 and MQTT 5
/// protocol versions. However svc-agent is based on [rumqtt](https://github.com/AtherEnergy/rumqtt)
/// MQTT client which only supports MQTT 3.1 because there's no pure Rust implementation of MQTT 5
/// client yet.
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
/// as a plain MQTT 5 messsage by an MQTT 5 client.
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
pub mod compat {
    use serde_derive::{Deserialize, Serialize};

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
    pub enum IncomingEnvelopeProperties {
        Event(IncomingEventProperties),
        Request(IncomingRequestProperties),
        Response(IncomingResponseProperties),
    }

    /// Incoming enveloped message.
    #[derive(Debug, Deserialize)]
    pub struct IncomingEnvelope {
        payload: String,
        properties: IncomingEnvelopeProperties,
    }

    impl IncomingEnvelope {
        pub fn properties(&self) -> &IncomingEnvelopeProperties {
            &self.properties
        }

        pub fn payload<T>(&self) -> Result<T, Error>
        where
            T: serde::de::DeserializeOwned,
        {
            let payload = serde_json::from_str::<T>(&self.payload).map_err(|e| {
                Error::new(&format!(
                    "error deserializing payload of an envelope, {}",
                    &e
                ))
            })?;
            Ok(payload)
        }
    }

    /// Parses an incoming envelope as an event with payload of type `T`.
    pub fn into_event<T>(envelope: IncomingEnvelope) -> Result<IncomingEvent<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            IncomingEnvelopeProperties::Event(props) => Ok(IncomingMessage::new(payload, props)),
            _ => Err(Error::new("error serializing an envelope into event")),
        }
    }

    /// Parses an incoming envelope as a request with payload of type `T`.
    pub fn into_request<T>(envelope: IncomingEnvelope) -> Result<IncomingRequest<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            IncomingEnvelopeProperties::Request(props) => Ok(IncomingMessage::new(payload, props)),
            _ => Err(Error::new("error serializing an envelope into request")),
        }
    }

    /// Parses an incoming envelope as a response with payload of type `T`.
    pub fn into_response<T>(envelope: IncomingEnvelope) -> Result<IncomingResponse<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = envelope.payload::<T>()?;
        match envelope.properties {
            IncomingEnvelopeProperties::Response(props) => Ok(IncomingMessage::new(payload, props)),
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
        properties: OutgoingEnvelopeProperties,
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
}

////////////////////////////////////////////////////////////////////////////////

/// An incoming MQTT notification.
///
/// Use it to process incoming messages.
/// See [AgentBuilder::start](struct.AgentBuilder.html#method.start) for details.
pub use rumqtt::client::Notification;

/// Quality of service that defines delivery guarantee level.
///
/// MQTT protocol defines three quality of service levels:
///
/// * 0 – at least once; no delivery guarantee.
/// * 1 – at most once; guaranteed to deliver but duplicates may arrive.
/// * 2 – exactly once; guaranteed to deliver only once.
///
/// The more the level – the more the performance overhead.
///
/// svc-agent sets QoS = 0 for outgoing events and responses and QoS = 1 for outgoing requests.
/// This means that only requests are guaranteed to be delivered to the broker but duplicates
/// are possible so maintaining request idempotency is up to agents.
pub use rumqtt::QoS;
