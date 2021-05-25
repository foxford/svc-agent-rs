use std::fmt;
use std::str::FromStr;

use async_channel::Sender;
use lazy_static::lazy_static;
use log::{debug, error, info};
use rumqttc::ConnAck;
use rumqttc::Connect;
use rumqttc::Event;
use rumqttc::Packet;
use rumqttc::PubAck;
use rumqttc::PubComp;
use rumqttc::PubRec;
use rumqttc::PubRel;
use rumqttc::SubAck;
use rumqttc::Subscribe;
use rumqttc::UnsubAck;
use rumqttc::Unsubscribe;
use rumqttc::{MqttOptions, Publish, Request};
use serde::{Deserialize, Serialize};

use super::*;
use crate::{AccountId, Addressable, AgentId, Authenticable, Error, SharedGroup};

#[cfg(feature = "queue-counter")]
use crate::queue_counter::QueueCounterHandle;

const DEFAULT_MQTT_REQUESTS_CHAN_SIZE: Option<usize> = Some(10_000);

lazy_static! {
    static ref TOKIO: tokio::runtime::Runtime = {
        let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
        rt_builder.enable_all();

        let thread_count = std::env::var("TOKIO_THREAD_COUNT").ok().map(|value| {
            value
                .parse::<usize>()
                .expect("Error converting TOKIO_THREAD_COUNT variable into usize")
        });

        if let Some(value) = thread_count {
            rt_builder.worker_threads(value);
        }

        rt_builder.build().expect("Failed to start tokio runtime")
    };
}

////////////////////////////////////////////////////////////////////////////////

/// Agent configuration.
///
/// # Options
///
/// * `uri` – MQTT broker URI (required).
/// * `clean_session` – whether to start a clean session or continue the persisted session.
/// Default: `true`.
/// * `keep_alive_interval` – keep alive time to ping the broker. Default: 30 sec.
/// * `reconnect_interval` – reconnection attempts interval, never reconnect if absent. Default: never reconnect.
/// * `outgoing_message_queue_size` – maximum messages in-flight. Default: 100.
/// * `incoming_message_queue_size` – notification channel capacity. Default: 10.
/// * `max_message_size` – maximum message size in bytes. Default: 256 * 1024.
/// * `password` – MQTT broker password.
/// * `requests_channel_size` - requests channel capacity.
#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    uri: String,
    clean_session: Option<bool>,
    keep_alive_interval: Option<u64>,
    reconnect_interval: Option<u64>,
    outgoing_message_queue_size: Option<usize>,
    incoming_message_queue_size: Option<usize>,
    password: Option<String>,
    max_message_size: Option<usize>,
    #[serde(default = "default_mqtt_requests_chan_size")]
    requests_channel_size: Option<usize>,
}

fn default_mqtt_requests_chan_size() -> Option<usize> {
    DEFAULT_MQTT_REQUESTS_CHAN_SIZE
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

    /// Starts an MQTT client and in case of successful connection returns a tuple containing
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
    ///         svc_agent::mqtt::AgentNotification::Message(message_result, message_metadata) => {
    ///             println!(
    ///                 "Incoming message: {:?} to topic {}",
    ///                 message_result,
    ///                 message_metadata.topic
    ///             );
    ///         }
    ///         _ => ()
    ///     }
    /// }
    /// ```
    pub fn start(
        self,
        config: &AgentConfig,
    ) -> Result<(Agent, crossbeam_channel::Receiver<AgentNotification>), Error> {
        self.start_with_runtime(config, TOKIO.handle().clone())
    }

    pub fn start_with_runtime(
        self,
        config: &AgentConfig,
        rt_handle: tokio::runtime::Handle,
    ) -> Result<(Agent, crossbeam_channel::Receiver<AgentNotification>), Error> {
        let options = Self::mqtt_options(&self.connection, &config)?;
        let channel_size = config
            .requests_channel_size
            .expect("requests_channel_size is not specified");
        let mut eventloop = rumqttc::EventLoop::new(options, channel_size);
        let mqtt_tx = eventloop.handle();
        let reconnect_interval = config.reconnect_interval.to_owned();
        let (tx, rx) = crossbeam_channel::unbounded::<AgentNotification>();
        #[cfg(feature = "queue-counter")]
        let queue_counter = QueueCounterHandle::start();
        #[cfg(feature = "queue-counter")]
        let queue_counter_ = queue_counter.clone();

        std::thread::Builder::new()
            .name("svc-agent-notifications-loop".to_owned())
            .spawn(move || {
                #[cfg(feature = "queue-counter")]
                let queue_counter_ = queue_counter_.clone();
                rt_handle.block_on(async {
                    loop {
                        match eventloop.poll().await {
                            Ok(packet) => match packet {
                                Event::Outgoing(content) => {
                                    info!("Outgoing message = '{:?}'", content);
                                }
                                Event::Incoming(message) => {
                                    debug!("Incoming item = {:?}", message);
                                    let mut msg: AgentNotification = message.into();
                                    if let AgentNotification::Message(Ok(ref mut content), _) = msg
                                    {
                                        if let IncomingMessage::Request(req) = content {
                                            let method = req.properties().method().to_owned();
                                            req.properties_mut().set_method(&method);
                                        }
                                        if let Err(e) = tx.send(msg) {
                                            error!("Failed to transmit message, reason = {}", e);
                                        };
                                        #[cfg(feature = "queue-counter")]
                                        queue_counter_.add_incoming_message(content);
                                    }
                                }
                            },
                            Err(err) => {
                                error!("Failed to poll, reason = {}", err);

                                if let Err(e) = tx.send(AgentNotification::Disconnection) {
                                    error!("Failed to notify about disconnection: {}", e);
                                }
                                match reconnect_interval {
                                    Some(value) => {
                                        tokio::time::sleep(std::time::Duration::from_secs(value))
                                            .await
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                });
            })
            .map_err(|e| {
                Error::new(&format!("Failed starting notifications loop thread, {}", e))
            })?;
        let agent = Agent::new(
            self.connection.agent_id,
            &self.api_version,
            mqtt_tx,
            #[cfg(feature = "queue-counter")]
            queue_counter,
        );

        Ok((agent, rx))
    }

    fn mqtt_options(connection: &Connection, config: &AgentConfig) -> Result<MqttOptions, Error> {
        let uri = config
            .uri
            .parse::<http::Uri>()
            .map_err(|e| Error::new(&format!("error parsing MQTT connection URL, {}", e)))?;
        let host = uri.host().ok_or_else(|| Error::new("missing MQTT host"))?;
        let port = uri.port().ok_or_else(|| Error::new("missing MQTT port"))?;

        // For MQTT 3 we specify connection version and mode in username field
        // because it doesn't have user properties like MQTT 5.
        let username = format!("{}::{}", connection.version, connection.mode);

        let password = config
            .password
            .to_owned()
            .unwrap_or_else(|| String::from(""));

        let mut opts = MqttOptions::new(connection.agent_id.to_string(), host, port.as_u16());
        opts.set_credentials(username, password);

        if let Some(value) = config.clean_session {
            opts.set_clean_session(value);
        }

        if let Some(value) = config.keep_alive_interval {
            opts.set_keep_alive(value as u16);
        }

        if let Some(value) = config.incoming_message_queue_size {
            opts.set_request_channel_capacity(value);
        }

        if let Some(value) = config.outgoing_message_queue_size {
            opts.set_inflight(value as u16);
        }

        if let Some(value) = config.max_message_size {
            opts.set_max_packet_size(value, value);
        };

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
    tx: Sender<Request>,
    #[cfg(feature = "queue-counter")]
    queue_counter: QueueCounterHandle,
}

impl Agent {
    #[cfg(feature = "queue-counter")]
    fn new(
        id: AgentId,
        api_version: &str,
        tx: Sender<Request>,
        queue_counter: QueueCounterHandle,
    ) -> Self {
        Self {
            address: Address::new(id, api_version),
            tx,
            queue_counter,
        }
    }

    #[cfg(not(feature = "queue-counter"))]
    fn new(id: AgentId, api_version: &str, tx: Sender<Request>) -> Self {
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
    ///     OutgoingShortTermTimingProperties::new(Utc::now()),
    /// );
    ///
    /// let message = OutgoingMessage::new(
    ///     json!({ "ping": "hello" }),
    ///     props,
    ///     Destination::Unicast(agent.id().clone(), "v1"),
    /// );
    ///
    /// agent.publish(message)?;
    /// ```
    pub fn publish<T: serde::Serialize>(
        &mut self,
        message: OutgoingMessage<T>,
    ) -> Result<(), Error> {
        let dump = Box::new(message).into_dump(&self.address)?;
        self.publish_dump(dump)
    }

    /// Publish a publishable message.
    ///
    /// # Arguments
    ///
    /// * `message` – message to publish.
    ///
    /// # Example
    ///
    /// ```
    /// let props = OutgoingRequestProperties::new(
    ///     "system.ping",
    ///     Subscription::unicast_responses_from(to).subscription_topic(agent.id(), "v1")?,
    ///     "random-string-123",
    ///     OutgoingShortTermTimingProperties::new(Utc::now()),
    /// );
    ///
    /// let message = OutgoingMessage::new(
    ///     json!({ "ping": "hello" }),
    ///     props,
    ///     Destination::Unicast(agent.id().clone(), "v1"),
    /// );
    ///
    /// let msg = Box::new(message) as Box<dyn IntoPublishableMessage>;
    /// agent.publish_publishable(msg.clone())?;
    /// println!("Message published: {}", msg);
    /// ```
    pub fn publish_publishable(
        &mut self,
        message: Box<dyn IntoPublishableMessage>,
    ) -> Result<(), Error> {
        let dump = message.into_dump(&self.address)?;
        self.publish_dump(dump)
    }

    pub fn publish_dump(&mut self, dump: PublishableMessage) -> Result<(), Error> {
        #[cfg(feature = "queue-counter")]
        self.queue_counter.add_outgoing_message(&dump);

        let dump = match dump {
            PublishableMessage::Event(dump) => dump,
            PublishableMessage::Request(dump) => dump,
            PublishableMessage::Response(dump) => dump,
        };

        info!(
            "Outgoing message = '{}' sending to the topic = '{}'",
            dump.payload(),
            dump.topic(),
        );

        let publish = Publish::new(dump.topic(), dump.qos(), dump.payload());

        self.tx.try_send(Request::Publish(publish)).map_err(|e| {
            if e.is_full() {
                error!(
                    "Rumq Requests channel reached maximum capacity, no space to publish, {:?}",
                    &e
                )
            }
            Error::new(&format!("error publishing MQTT message, {}", &e))
        })
    }

    /// Subscribe to a topic.
    ///
    /// Note that the subscription is actually gets confirmed on receiving
    /// `AgentNotification::Suback` notification.
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
    ///
    /// match rx.recv_timeout(Duration::from_secs(5)) {
    ///     Ok(AgentNotification::Suback(_)) => (),
    ///     Ok(other) => panic!("Expected to receive suback notification, got {:?}", other),
    ///     Err(err) => panic!("Failed to receive suback notification: {}", err),
    /// }
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
            .try_send(Request::Subscribe(Subscribe::new(topic, qos)))
            .map_err(|e| Error::new(&format!("error creating MQTT subscription, {}", e)))?;

        Ok(())
    }

    #[cfg(feature = "queue-counter")]
    pub fn get_queue_counter(&self) -> QueueCounterHandle {
        self.queue_counter.clone()
    }
}

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
                let version = (*version_str).to_string();
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
    pub(crate) fn to_connection(&self) -> Connection {
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

/// An incoming MQTT notification.
///
/// Use it to process incoming messages.
/// See [AgentBuilder::start](struct.AgentBuilder.html#method.start) for details.
#[derive(Debug)]
pub enum AgentNotification {
    Message(Result<IncomingMessage<String>, String>, MessageData),
    Reconnection,
    Disconnection,
    Puback(PubAck),
    Pubrec(PubRec),
    Pubcomp(PubComp),
    Suback(SubAck),
    Unsuback(UnsubAck),
    Connect(Connect),
    Connack(ConnAck),
    Pubrel(PubRel),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    PingReq,
    PingResp,
    Disconnect,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageData {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: String,
    pub pkid: u16,
}

impl From<Packet> for AgentNotification {
    fn from(notification: Packet) -> Self {
        match notification {
            Packet::Publish(message) => {
                let message_data = MessageData {
                    dup: message.dup,
                    qos: message.qos,
                    retain: message.retain,
                    topic: message.topic,
                    pkid: message.pkid,
                };

                let env_result =
                    serde_json::from_slice::<compat::IncomingEnvelope>(&message.payload)
                        .map_err(|err| format!("Failed to parse incoming envelope: {}", err))
                        .and_then(|env| match env.properties() {
                            compat::IncomingEnvelopeProperties::Request(_) => {
                                compat::into_request(env)
                                    .map_err(|e| format!("Failed to convert into request: {}", e))
                            }
                            compat::IncomingEnvelopeProperties::Response(_) => {
                                compat::into_response(env)
                                    .map_err(|e| format!("Failed to convert into response: {}", e))
                            }
                            compat::IncomingEnvelopeProperties::Event(_) => compat::into_event(env)
                                .map_err(|e| format!("Failed to convert into event: {}", e)),
                        });

                Self::Message(env_result, message_data)
            }
            Packet::PubAck(p) => Self::Puback(p),
            Packet::PubRec(p) => Self::Pubrec(p),
            Packet::PubComp(p) => Self::Pubcomp(p),
            Packet::SubAck(s) => Self::Suback(s),
            Packet::UnsubAck(p) => Self::Unsuback(p),
            Packet::Connect(connect) => Self::Connect(connect),
            Packet::ConnAck(conn_ack) => Self::Connack(conn_ack),
            Packet::PubRel(pub_rel) => Self::Pubrel(pub_rel),
            Packet::Subscribe(sub) => Self::Subscribe(sub),
            Packet::Unsubscribe(unsub) => Self::Unsubscribe(unsub),
            Packet::PingReq => Self::PingReq,
            Packet::PingResp => Self::PingResp,
            Packet::Disconnect => Self::Disconnect,
        }
    }
}
