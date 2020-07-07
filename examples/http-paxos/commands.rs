use bincode::{deserialize, serialize};
use bytes::Bytes;
use hyper::{client::HttpConnector, Body, Client, Request};
use paxos::{Command, NodeId, NodeMetadata, Receiver, Transport};

pub struct HttpTransport {
    client: Client<HttpConnector, Body>,
}

impl Default for HttpTransport {
    fn default() -> HttpTransport {
        HttpTransport { client: Client::new() }
    }
}

impl Transport for HttpTransport {
    fn send(&mut self, _node: NodeId, meta: &NodeMetadata, cmd: Command) {
        let bytes = match serialize(&cmd) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Error serializing command: {:?}", e);
                return;
            }
        };

        let request =
            Request::builder().method("POST").uri(meta.0.as_ref()).body(bytes.into()).unwrap();
        tokio::spawn(self.client.request(request));
    }
}

pub fn invoke<C: Receiver>(replica: &mut C, command: Bytes) {
    let cmd = match deserialize(&command) {
        Ok(cmd) => cmd,
        Err(_) => return,
    };
    replica.receive(cmd);
}
