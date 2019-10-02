use serde_derive::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::{
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
    username: Option<String>,
    password: Option<String>,
}

impl AgentConfig {
    pub fn set_username(&mut self, value: &str) -> &mut Self {
        self.username = Some(value.to_owned());
        self
    }

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
        let options = Self::mqtt_options(&self.connection.to_string(), &config)?;
        let (tx, rx) = rumqtt::MqttClient::start(options)
            .map_err(|e| Error::new(&format!("error starting MQTT client, {}", e)))?;

        let agent = Agent::new(self.connection.agent_id, tx);
        Ok((agent, rx))
    }

    fn mqtt_options(client_id: &str, config: &AgentConfig) -> Result<rumqtt::MqttOptions, Error> {
        let uri = config
            .uri
            .parse::<http::Uri>()
            .map_err(|e| Error::new(&format!("error parsing MQTT connection URL, {}", e)))?;
        let host = uri.host().ok_or_else(|| Error::new("missing MQTT host"))?;
        let port = uri
            .port_part()
            .ok_or_else(|| Error::new("missing MQTT port"))?;

        // TODO: change to convenient code as soon as the PR will be merged
        // https://github.com/AtherEnergy/rumqtt/pull/145
        let mut opts = rumqtt::MqttOptions::new(client_id, host, port.as_u16());
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
        opts = match (&config.username, &config.password) {
            (Some(ref u), Some(ref p)) => opts.set_security_opts(
                rumqtt::SecurityOptions::UsernamePassword(u.to_owned(), p.to_owned()),
            ),
            (Some(ref u), None) => opts.set_security_opts(
                rumqtt::SecurityOptions::UsernamePassword(u.to_owned(), String::from("")),
            ),
            (None, Some(ref p)) => opts.set_security_opts(
                rumqtt::SecurityOptions::UsernamePassword(String::from(""), p.to_owned()),
            ),
            (None, None) => opts,
        };

        Ok(opts)
    }
}

#[derive(Clone)]
pub struct Agent {
    id: AgentId,
    tx: rumqtt::MqttClient,
}

impl Agent {
    fn new(id: AgentId, tx: rumqtt::MqttClient) -> Self {
        Self { id, tx }
    }

    pub fn id(&self) -> &AgentId {
        &self.id
    }

    pub fn publish(&mut self, message: Box<dyn Publishable>) -> Result<(), Error> {
        let topic = self.id.destination_topic(&message)?;
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
        let mut topic = subscription.subscription_topic(&self.id)?;
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
                ConnectionMode::Default => "agents",
                ConnectionMode::Service => "service-agents",
                ConnectionMode::Observer => "observer-agents",
                ConnectionMode::Bridge => "bridge-agents",
            }
        )
    }
}

impl FromStr for ConnectionMode {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "agents" => Ok(ConnectionMode::Default),
            "service-agents" => Ok(ConnectionMode::Service),
            "observer-agents" => Ok(ConnectionMode::Observer),
            "bridge-agents" => Ok(ConnectionMode::Bridge),
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
            version: String::from("v1"),
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
    #[serde(flatten)]
    authn: AuthnProperties,
    #[serde(rename = "connection_version")]
    version: String,
    #[serde(rename = "connection_mode")]
    mode: ConnectionMode,
}

impl ConnectionProperties {
    fn to_connection(&self) -> Connection {
        let mut connection = Connection::new(self.authn.as_agent_id().clone());
        connection.set_version(&self.version);
        connection.set_mode(self.mode.clone());
        connection
    }
}

impl Authenticable for ConnectionProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.authn.as_account_id()
    }
}

impl Addressable for ConnectionProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.authn.as_agent_id()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct BrokerProperties {
    agent_id: AgentId,
}

impl From<AgentId> for BrokerProperties {
    fn from(agent_id: AgentId) -> Self {
        Self { agent_id }
    }
}

impl Authenticable for BrokerProperties {
    fn as_account_id(&self) -> &AccountId {
        self.agent_id.as_account_id()
    }
}

impl Addressable for BrokerProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.agent_id
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthnProperties {
    agent_id: AgentId,
}

impl Authenticable for AuthnProperties {
    fn as_account_id(&self) -> &AccountId {
        &self.agent_id.as_account_id()
    }
}

impl Addressable for AuthnProperties {
    fn as_agent_id(&self) -> &AgentId {
        &self.agent_id
    }
}

impl From<AgentId> for AuthnProperties {
    fn from(agent_id: AgentId) -> Self {
        Self { agent_id }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct IncomingEventProperties {
    #[serde(flatten)]
    conn: ConnectionProperties,
    label: Option<String>,
}

impl IncomingEventProperties {
    pub fn to_connection(&self) -> Connection {
        self.conn.to_connection()
    }

    pub fn label(&self) -> Option<&str> {
        self.label.as_ref().map(|l| &**l)
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
    #[serde(flatten)]
    broker: BrokerProperties,
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

    pub fn broker(&self) -> &BrokerProperties {
        &self.broker
    }

    pub fn to_connection(&self) -> Connection {
        self.conn.to_connection()
    }

    pub fn to_response(&self, status: ResponseStatus) -> OutgoingResponseProperties {
        OutgoingResponseProperties::new(status, &self.correlation_data)
            .set_response_topic(&self.response_topic)
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
}

impl IncomingResponseProperties {
    pub fn status(&self) -> ResponseStatus {
        self.status
    }

    pub fn correlation_data(&self) -> &str {
        &self.correlation_data
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
    pub fn to_response<R>(&self, data: R, status: ResponseStatus) -> OutgoingResponse<R>
    where
        R: serde::Serialize,
    {
        OutgoingMessage::new(
            data,
            self.properties.to_response(status),
            Destination::Unicast(self.properties().as_agent_id().clone()),
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
}

impl OutgoingEventProperties {
    pub fn new(label: &'static str) -> Self {
        Self { label }
    }
}

#[derive(Debug, Serialize)]
pub struct OutgoingRequestProperties {
    method: String,
    correlation_data: String,
    response_topic: String,
    #[serde(flatten)]
    authn: Option<AuthnProperties>,
}

impl OutgoingRequestProperties {
    pub fn new(method: &str, response_topic: &str, correlation_data: &str) -> Self {
        Self {
            method: method.to_owned(),
            response_topic: response_topic.to_owned(),
            correlation_data: correlation_data.to_owned(),
            authn: None,
        }
    }

    pub fn set_authn(&mut self, authn: AuthnProperties) -> &mut Self {
        self.authn = Some(authn);
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
}

impl OutgoingResponseProperties {
    pub fn new(status: ResponseStatus, correlation_data: &str) -> Self {
        Self {
            status,
            correlation_data: correlation_data.to_owned(),
            response_topic: None,
        }
    }

    pub fn set_response_topic(self, response_topic: &str) -> Self {
        Self {
            response_topic: Some(response_topic.to_string()),
            ..self
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

    pub fn unicast<A>(payload: T, properties: OutgoingRequestProperties, to: &A) -> Self
    where
        A: Addressable,
    {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Unicast(to.as_agent_id().clone()),
        )
    }
}

impl<T> OutgoingResponse<T>
where
    T: serde::Serialize,
{
    pub fn unicast<A>(payload: T, properties: OutgoingResponseProperties, to: &A) -> Self
    where
        A: Addressable,
    {
        OutgoingMessage::new(
            payload,
            properties,
            Destination::Unicast(to.as_agent_id().clone()),
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
    fn destination_topic(&self, message: &Box<dyn Publishable>) -> Result<String, Error>;
}

impl DestinationTopic for AgentId {
    fn destination_topic(&self, message: &Box<dyn Publishable>) -> Result<String, Error> {
        let dest = message.destination();

        match message.message_type() {
            "event" => match dest {
                Destination::Broadcast(ref uri) => Ok(format!(
                    "apps/{app}/api/v1/{uri}",
                    app = self.as_account_id(),
                    uri = uri,
                )),
                _ => Err(Error::new(&format!(
                    "destination = '{:?}' is incompatible with event message type",
                    dest,
                ))),
            },
            "request" => match dest {
                Destination::Unicast(ref agent_id) => Ok(format!(
                    "agents/{agent_id}/api/v1/in/{app}",
                    agent_id = agent_id,
                    app = self.as_account_id(),
                )),
                Destination::Multicast(ref account_id) => Ok(format!(
                    "agents/{agent_id}/api/v1/out/{app}",
                    agent_id = self,
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
                    Destination::Unicast(ref agent_id) => Ok(format!(
                        "agents/{agent_id}/api/v1/in/{app}",
                        agent_id = agent_id,
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
    fn subscription_topic<A>(&self, agent_id: &A) -> Result<String, Error>
    where
        A: Addressable;
}

impl<'a> SubscriptionTopic for EventSubscription<'a> {
    fn subscription_topic<A>(&self, _me: &A) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Broadcast(ref from_account_id, ref uri) => Ok(format!(
                "apps/{app}/api/v1/{uri}",
                app = from_account_id,
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
    fn subscription_topic<A>(&self, me: &A) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Multicast(Some(ref from_agent_id)) => Ok(format!(
                "agents/{agent_id}/api/v1/out/{app}",
                agent_id = from_agent_id,
                app = me.as_account_id(),
            )),
            Source::Multicast(None) => Ok(format!(
                "agents/+/api/v1/out/{app}",
                app = me.as_account_id(),
            )),
            Source::Unicast(Some(ref from_account_id)) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app}",
                agent_id = me.as_agent_id(),
                app = from_account_id,
            )),
            Source::Unicast(None) => Ok(format!(
                "agents/{agent_id}/api/v1/in/+",
                agent_id = me.as_agent_id(),
            )),
            _ => Err(Error::new(&format!(
                "source = '{:?}' is incompatible with request subscription",
                self.source,
            ))),
        }
    }
}

impl<'a> SubscriptionTopic for ResponseSubscription<'a> {
    fn subscription_topic<A>(&self, me: &A) -> Result<String, Error>
    where
        A: Addressable,
    {
        match self.source {
            Source::Unicast(Some(ref from_account_id)) => Ok(format!(
                "agents/{agent_id}/api/v1/in/{app}",
                agent_id = me.as_agent_id(),
                app = from_account_id,
            )),
            Source::Unicast(None) => Ok(format!(
                "agents/{agent_id}/api/v1/in/+",
                agent_id = me.as_agent_id(),
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
