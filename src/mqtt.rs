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
    pub fn set_password(&mut self, value: &str) -> &mut Self {
        self.password = Some(value.to_owned());
        self
    }
}

#[derive(Debug)]
pub struct AgentBuilder {
    connection: Connection,
}

impl AgentBuilder {
    pub fn new(agent_id: AgentId) -> Self {
        Self {
            connection: Connection::new(agent_id),
        }
    }

    pub fn version(self, version: &str) -> Self {
        let mut connection = self.connection;
        connection.set_version(version);
        Self {
            connection: connection,
        }
    }

    pub fn mode(self, mode: ConnectionMode) -> Self {
        let mut connection = self.connection;
        connection.set_mode(mode);
        Self {
            connection: connection,
        }
    }

    pub fn start(
        self,
        config: &AgentConfig,
    ) -> Result<(Agent, rumqtt::Receiver<rumqtt::Notification>), Error> {
        let options = Self::mqtt_options(&self.connection, &config)?;
        let (tx, rx) = rumqtt::MqttClient::start(options)
            .map_err(|e| Error::new(&format!("error starting MQTT client, {}", e)))?;

        let agent = Agent::new(self.connection.agent_id, &self.connection.version, tx);
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

#[derive(Clone)]
pub struct Agent {
    id: AgentId,
    version: String,
    tx: rumqtt::MqttClient,
}

impl Agent {
    fn new(id: AgentId, version: &str, tx: rumqtt::MqttClient) -> Self {
        Self {
            id,
            version: version.to_owned(),
            tx,
        }
    }

    pub fn id(&self) -> &AgentId {
        &self.id
    }

    pub fn publish(&mut self, message: Box<dyn Publishable>) -> Result<(), Error> {
        let topic = self.id.destination_topic(&message, &self.version)?;
        let qos = message.qos();
        let bytes = message.into_bytes()?;

        self.tx
            .publish(topic, qos, false, bytes)
            .map_err(|e| Error::new(&format!("error publishing MQTT message, {}", &e)))
    }

    pub fn subscribe<S>(
        &mut self,
        subscription: &S,
        qos: QoS,
        maybe_group: Option<&SharedGroup>,
    ) -> Result<(), Error>
    where
        S: SubscriptionTopic,
    {
        let mut topic = subscription.subscription_topic(&self.id, &self.version)?;
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

#[derive(Debug, Clone)]
pub enum ConnectionMode {
    Default,
    Service,
    Observer,
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
    pub fn until_now(start_timestamp: DateTime<Utc>) -> Self {
        let now = Utc::now();
        let mut timing = Self::new(now);
        timing.set_processing_time(now - start_timestamp);
        timing
    }

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

#[derive(Clone, Debug)]
pub struct SessionId {
    agent_session_label: Uuid,
    broker_session_label: Uuid,
}

impl FromStr for SessionId {
    type Err = Error;

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
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}.{}",
            self.agent_session_label, self.broker_session_label
        )
    }
}

#[derive(Clone, Debug)]
pub struct TrackingId {
    label: Uuid,
    session_id: SessionId,
}

impl FromStr for TrackingId {
    type Err = Error;

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
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}.{}", self.label, self.session_id)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackingProperties {
    tracking_id: TrackingId,
    #[serde(with = "session_ids_list")]
    session_tracking_label: Vec<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_tracking_label: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////

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

#[derive(Debug, Deserialize)]
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

#[derive(Debug)]
pub struct IncomingMessage<T, P>
where
    P: Addressable,
{
    payload: T,
    properties: P,
}

impl<T, P> IncomingMessage<T, P>
where
    P: Addressable,
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
    pub fn to_response<R>(
        &self,
        data: R,
        status: ResponseStatus,
        timing: ShortTermTimingProperties,
    ) -> OutgoingResponse<R>
    where
        R: serde::Serialize,
    {
        OutgoingMessage::new(
            data,
            self.properties.to_response(status, timing),
            Destination::Unicast(
                self.properties.as_agent_id().clone(),
                self.properties.to_connection().version().to_owned(),
            ),
        )
    }
}

pub type IncomingEvent<T> = IncomingMessage<T, IncomingEventProperties>;
pub type IncomingRequest<T> = IncomingMessage<T, IncomingRequestProperties>;
pub type IncomingResponse<T> = IncomingMessage<T, IncomingResponseProperties>;

////////////////////////////////////////////////////////////////////////////////

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

#[derive(Debug, Serialize)]
pub struct OutgoingRequestProperties {
    method: String,
    correlation_data: String,
    response_topic: String,
    agent_id: Option<AgentId>,
    #[serde(flatten)]
    long_term_timing: Option<LongTermTimingProperties>,
    #[serde(flatten)]
    short_term_timing: ShortTermTimingProperties,
    #[serde(flatten)]
    tracking: Option<TrackingProperties>,
}

impl OutgoingRequestProperties {
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

pub type ResponseStatus = http::StatusCode;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct OutgoingMessage<T, P>
where
    T: serde::Serialize,
    P: OutgoingProperties,
{
    payload: T,
    properties: P,
    destination: Destination,
}

impl<T, P> OutgoingMessage<T, P>
where
    T: serde::Serialize,
    P: OutgoingProperties,
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
    fn message_type(&self) -> &'static str;
    fn destination(&self) -> &Destination;
    fn qos(&self) -> QoS;
    fn response_topic(&self) -> Option<&str>;
    fn into_bytes(self: Box<Self>) -> Result<String, Error>;
}

impl<T, P> Publishable for OutgoingMessage<T, P>
where
    T: serde::Serialize,
    P: OutgoingProperties,
    OutgoingMessage<T, P>: compat::IntoEnvelope,
{
    fn message_type(&self) -> &'static str {
        self.properties.message_type()
    }

    fn destination(&self) -> &Destination {
        &self.destination
    }

    fn qos(&self) -> QoS {
        self.properties.qos()
    }

    fn response_topic(&self) -> Option<&str> {
        self.properties.response_topic()
    }

    fn into_bytes(self: Box<Self>) -> Result<String, Error> {
        use compat::IntoEnvelope;

        let envelope = self.into_envelope()?;

        Ok(serde_json::to_string(&envelope)
            .map_err(|e| Error::new(&format!("error serializing an envelope to bytes, {}", &e)))?)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub trait DestinationTopic {
    fn destination_topic(
        &self,
        message: &Box<dyn Publishable>,
        me_version: &str,
    ) -> Result<String, Error>;
}

impl DestinationTopic for AgentId {
    fn destination_topic(
        &self,
        message: &Box<dyn Publishable>,
        me_version: &str,
    ) -> Result<String, Error> {
        let dest = message.destination();

        match message.message_type() {
            "event" => match dest {
                Destination::Broadcast(ref uri) => Ok(format!(
                    "apps/{app}/api/{version}/{uri}",
                    app = self.as_account_id(),
                    version = me_version,
                    uri = uri,
                )),
                _ => Err(Error::new(&format!(
                    "destination = '{:?}' is incompatible with event message type",
                    dest,
                ))),
            },
            "request" => match dest {
                Destination::Unicast(ref agent_id, ref version) => Ok(format!(
                    "agents/{agent_id}/api/{version}/in/{app}",
                    agent_id = agent_id,
                    version = version,
                    app = self.as_account_id(),
                )),
                Destination::Multicast(ref account_id) => Ok(format!(
                    "agents/{agent_id}/api/{version}/out/{app}",
                    agent_id = self,
                    version = me_version,
                    app = account_id,
                )),
                _ => Err(Error::new(&format!(
                    "destination = '{:?}' is incompatible with request message type",
                    dest,
                ))),
            },
            "response" => match message.response_topic() {
                Some(val) => Ok(val.to_string()),
                None => match dest {
                    Destination::Unicast(ref agent_id, ref version) => Ok(format!(
                        "agents/{agent_id}/api/{version}/in/{app}",
                        agent_id = agent_id,
                        version = version,
                        app = self.as_account_id(),
                    )),
                    _ => Err(Error::new(&format!(
                        "destination = '{:?}' is incompatible with response message type",
                        dest,
                    ))),
                },
            },
            message_type => Err(Error::new(&format!(
                "Unknown message type: '{}'",
                message_type
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub trait OutgoingProperties {
    fn message_type(&self) -> &'static str;
    fn qos(&self) -> QoS;
    fn response_topic(&self) -> Option<&str>;
}

impl OutgoingProperties for OutgoingEventProperties {
    fn message_type(&self) -> &'static str {
        "event"
    }

    fn qos(&self) -> QoS {
        QoS::AtLeastOnce
    }

    fn response_topic(&self) -> Option<&str> {
        None
    }
}

impl OutgoingProperties for OutgoingRequestProperties {
    fn message_type(&self) -> &'static str {
        "request"
    }

    fn qos(&self) -> QoS {
        QoS::AtMostOnce
    }

    fn response_topic(&self) -> Option<&str> {
        None
    }
}

impl OutgoingProperties for OutgoingResponseProperties {
    fn message_type(&self) -> &'static str {
        "response"
    }

    fn qos(&self) -> QoS {
        QoS::AtLeastOnce
    }

    fn response_topic(&self) -> Option<&str> {
        self.response_topic()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub trait SubscriptionTopic {
    fn subscription_topic<A>(&self, agent_id: &A, me_version: &str) -> Result<String, Error>
    where
        A: Addressable;
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

pub mod compat {
    use serde_derive::{Deserialize, Serialize};

    use super::{
        Destination, IncomingEvent, IncomingEventProperties, IncomingMessage, IncomingRequest,
        IncomingRequestProperties, IncomingResponse, IncomingResponseProperties,
        OutgoingEventProperties, OutgoingRequestProperties, OutgoingResponseProperties,
    };
    use crate::Error;

    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    #[serde(tag = "type")]
    pub enum IncomingEnvelopeProperties {
        Event(IncomingEventProperties),
        Request(IncomingRequestProperties),
        Response(IncomingResponseProperties),
    }

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

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "lowercase")]
    #[serde(tag = "type")]
    pub enum OutgoingEnvelopeProperties {
        Event(OutgoingEventProperties),
        Request(OutgoingRequestProperties),
        Response(OutgoingResponseProperties),
    }

    #[derive(Debug, Serialize)]
    pub struct OutgoingEnvelope {
        payload: String,
        properties: OutgoingEnvelopeProperties,
        #[serde(skip)]
        destination: Destination,
    }

    impl OutgoingEnvelope {
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
        fn into_envelope(self) -> Result<OutgoingEnvelope, Error>;
    }
}

////////////////////////////////////////////////////////////////////////////////

pub use rumqtt::client::Notification;
pub use rumqtt::QoS;
