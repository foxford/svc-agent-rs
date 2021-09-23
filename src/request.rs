use std::{collections::HashMap, sync::Mutex};

use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::oneshot;

use crate::{
    mqtt::{Agent, IncomingResponse, OutgoingMessage, OutgoingRequest},
    Error,
};

pub struct Dispatcher {
    agent: Agent,
    store: Mutex<HashMap<String, oneshot::Sender<IncomingResponse<JsonValue>>>>,
}

impl Dispatcher {
    pub fn new(agent: &Agent) -> Self {
        Self {
            agent: agent.to_owned(),
            store: Mutex::new(HashMap::new()),
        }
    }

    pub async fn request<Req, Resp>(
        &self,
        req: OutgoingRequest<Req>,
    ) -> Result<IncomingResponse<Resp>, Error>
    where
        Req: 'static + Serialize,
        Resp: DeserializeOwned,
    {
        let corr_data = req.properties().correlation_data();
        let mut store_lock = self.store.lock().expect("Dispatcher lock poisoned");

        if store_lock.get(corr_data).is_some() {
            let err = format!(
                "Already awaiting response with correlation data = '{}'",
                corr_data
            );
            return Err(Error::new(&err));
        }

        let (tx, rx) = oneshot::channel::<IncomingResponse<JsonValue>>();
        store_lock.insert(corr_data.to_owned(), tx);
        drop(store_lock);

        self.agent.clone().publish(OutgoingMessage::Request(req))?;

        let resp = rx
            .await
            .map_err(|err| Error::new(&format!("Failed to receive response: {}", err)))?;

        let props = resp.properties().to_owned();
        let payload = serde_json::from_value::<Resp>(resp.payload().to_owned())
            .map_err(|err| Error::new(&format!("Failed to parse response payload: {}", err)))?;

        Ok(IncomingResponse::new(payload, props))
    }

    pub async fn response(&self, resp: IncomingResponse<JsonValue>) -> Result<(), Error> {
        let mut store_lock = self.store.lock().expect("Dispatcher lock poisoned");

        let tx = store_lock
            .remove(resp.properties().correlation_data())
            .ok_or_else(|| {
                Error::new(&format!(
                    "Failed to commit response with correlation data = '{}': not being awaited",
                    resp.properties().correlation_data()
                ))
            })?;

        drop(store_lock);

        tx.send(resp).map_err(|resp| {
            Error::new(&format!(
                "Failed to commit response with correlation data = '{}': receiver has been dropped",
                resp.properties().correlation_data(),
            ))
        })?;

        Ok(())
    }

    pub async fn cancel_request(&self, corr_data: &str) -> Result<(), Error> {
        self.store
            .lock()
            .expect("Dispatcher lock poisoned")
            .remove(corr_data)
            .map(|_| ())
            .ok_or_else(|| Error::new(&format!(
                "Failed to cancel request; response with correlation data = '{}' is not being awaited",
                corr_data
            )))
    }
}
