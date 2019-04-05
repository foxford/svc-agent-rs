#[cfg_attr(feature = "diesel", macro_use)]
#[cfg(feature = "diesel")]
extern crate diesel;

use std::fmt;
use std::str::FromStr;

////////////////////////////////////////////////////////////////////////////////

pub trait Addressable: Authenticable {
    fn as_agent_id(&self) -> &AgentId;
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "diesel", derive(FromSqlRow, AsExpression))]
#[cfg_attr(feature = "diesel", sql_type = "sql::Agent_id")]
pub struct AgentId {
    label: String,
    account_id: AccountId,
}

impl AgentId {
    pub fn new(label: &str, account_id: AccountId) -> Self {
        Self {
            label: label.to_owned(),
            account_id,
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}.{}", self.label(), self.account_id)
    }
}

impl FromStr for AgentId {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(2, '.').collect();
        match parts[..] {
            [ref label, ref rest] => {
                let account_id = rest.parse::<AccountId>().map_err(|e| {
                    Error::new(&format!(
                        "error deserializing shared group from a string, {}",
                        &e
                    ))
                })?;
                Ok(Self::new(label, account_id))
            }
            _ => Err(Error::new(&format!(
                "invalid value for the agent id: {}",
                val
            ))),
        }
    }
}

impl Authenticable for AgentId {
    fn as_account_id(&self) -> &AccountId {
        &self.account_id
    }
}

impl Addressable for AgentId {
    fn as_agent_id(&self) -> &Self {
        self
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SharedGroup {
    label: String,
    account_id: AccountId,
}

impl SharedGroup {
    pub fn new(label: &str, account_id: AccountId) -> Self {
        Self {
            label: label.to_owned(),
            account_id,
        }
    }
}

impl fmt::Display for SharedGroup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.label, self.account_id)
    }
}

impl FromStr for SharedGroup {
    type Err = Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = val.splitn(2, '.').collect();
        match parts[..] {
            [ref label, ref rest] => {
                let account_id = rest.parse::<AccountId>().map_err(|e| {
                    Error::new(&format!(
                        "error deserializing shared group from a string, {}",
                        &e
                    ))
                })?;
                Ok(Self::new(label, account_id))
            }
            _ => Err(Error::new(&format!(
                "invalid value for the application group: {}",
                val
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum Destination {
    // -> event(app-to-any): apps/ACCOUNT_ID(ME)/api/v1/BROADCAST_URI
    Broadcast(String),
    // -> request(one-to-app): agents/AGENT_ID(ME)/api/v1/out/ACCOUNT_ID
    Multicast(AccountId),
    // -> request(one-to-one): agents/AGENT_ID/api/v1/in/ACCOUNT_ID(ME)
    // -> response(one-to-one): agents/AGENT_ID/api/v1/in/ACCOUNT_ID(ME)
    Unicast(AgentId),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum Source<'a> {
    // <- event(any-from-app): apps/ACCOUNT_ID/api/v1/BROADCAST_URI
    Broadcast(&'a AccountId, &'a str),
    // <- request(app-from-any): agents/+/api/v1/out/ACCOUNT_ID(ME)
    Multicast,
    // <- request(one-from-one): agents/AGENT_ID(ME)/api/v1/in/ACCOUNT_ID
    // <- request(one-from-any): agents/AGENT_ID(ME)/api/v1/in/+
    // <- response(one-from-one): agents/AGENT_ID(ME)/api/v1/in/ACCOUNT_ID
    // <- response(one-from-any): agents/AGENT_ID(ME)/api/v1/in/+
    Unicast(Option<&'a AccountId>),
}

////////////////////////////////////////////////////////////////////////////////

pub struct Subscription {}

impl Subscription {
    pub fn broadcast_events<'a, A>(from: &'a A, uri: &'a str) -> EventSubscription<'a>
    where
        A: Authenticable,
    {
        EventSubscription::new(Source::Broadcast(from.as_account_id(), uri))
    }

    pub fn multicast_requests<'a>() -> RequestSubscription<'a> {
        RequestSubscription::new(Source::Multicast)
    }

    pub fn unicast_requests<A>(from: Option<&A>) -> RequestSubscription
    where
        A: Authenticable,
    {
        RequestSubscription::new(Source::Unicast(from.map(|val| val.as_account_id())))
    }

    pub fn unicast_responses<A>(from: Option<&A>) -> ResponseSubscription
    where
        A: Authenticable,
    {
        ResponseSubscription::new(Source::Unicast(from.map(|val| val.as_account_id())))
    }
}

pub struct EventSubscription<'a> {
    source: Source<'a>,
}

impl<'a> EventSubscription<'a> {
    pub fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

pub struct RequestSubscription<'a> {
    source: Source<'a>,
}

impl<'a> RequestSubscription<'a> {
    pub fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

pub struct ResponseSubscription<'a> {
    source: Source<'a>,
}

impl<'a> ResponseSubscription<'a> {
    pub fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "diesel")]
pub mod sql {
    use super::{AccountId, AgentId};

    use diesel::deserialize::{self, FromSql};
    use diesel::pg::Pg;
    use diesel::serialize::{self, Output, ToSql, WriteTuple};
    use diesel::sql_types::{Record, Text};
    use std::io::Write;

    #[derive(SqlType, QueryId)]
    #[postgres(type_name = "agent_id")]
    #[allow(non_camel_case_types)]
    pub struct Agent_id;

    impl ToSql<Agent_id, Pg> for AgentId {
        fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
            WriteTuple::<(Account_id, Text)>::write_tuple(&(&self.account_id, &self.label), out)
        }
    }

    impl FromSql<Agent_id, Pg> for AgentId {
        fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
            let (account_id, label): (AccountId, String) =
                FromSql::<Record<(Account_id, Text)>, Pg>::from_sql(bytes)?;
            Ok(AgentId::new(&label, account_id))
        }
    }

    pub use svc_authn::sql::Account_id;
}

////////////////////////////////////////////////////////////////////////////////

pub use svc_authn::{AccountId, Authenticable};

pub use self::error::Error;
pub mod error;
pub mod mqtt;
pub(crate) mod serde;
