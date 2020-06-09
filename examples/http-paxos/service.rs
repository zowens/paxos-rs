use crate::{commands, kvstore::KvCommand};
use bytes::Bytes;
use hyper::{Body, Method, Request, Response, StatusCode};
use paxos::{Commander, Sender};
use rand::random;
use std::{sync::Arc, time::Duration};
use tokio::{self, sync::Mutex, task::JoinHandle, time::interval};

#[derive(Clone)]
pub struct Handler {
    replica: Arc<Mutex<paxos::Replica<commands::PaxosSender>>>,
}

impl Handler {
    pub fn new(replica: paxos::Replica<commands::PaxosSender>) -> Handler {
        Handler { replica: Arc::new(Mutex::new(replica)) }
    }

    /// start a cleanup loop to handle closed clients
    pub fn spawn_cleanup_loop(&self) -> JoinHandle<()> {
        let replica_arch_timer = self.replica.clone();
        tokio::spawn(async move {
            let mut ticks = interval(Duration::new(30, 0));
            loop {
                ticks.tick().await;

                let mut replica = replica_arch_timer.lock().await;
                replica.sender_mut().state_machine().prune_listeners();
            }
        })
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let path = Bytes::from(req.uri().path()[1..].to_string());
        match (req.method(), path) {
            (&Method::POST, key) if key == "paxos" => {
                {
                    let cmd = hyper::body::to_bytes(req.into_body()).await?;
                    let mut replica = self.replica.lock().await;
                    commands::invoke(&mut replica, cmd);
                }

                respond(StatusCode::ACCEPTED)
            }
            (&Method::POST, key) => {
                let value = hyper::body::to_bytes(req.into_body()).await?;
                let id = random::<u64>();
                let receiver = {
                    let mut replica = self.replica.lock().await;
                    let receiver = replica.sender_mut().state_machine().register_set(id);
                    replica.proposal(KvCommand::Set { request_id: id, key, value }.into());
                    receiver
                };

                match receiver.await {
                    Ok(slot) => Ok(Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .header("X-Paxos-Slot", slot)
                        .body(Body::empty())
                        .unwrap()),
                    Err(_) => respond(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            (&Method::GET, key) => {
                let id = random::<u64>();
                let receiver = {
                    let mut replica = self.replica.lock().await;
                    let receiver = replica.sender_mut().state_machine().register_get(id);
                    replica.proposal(KvCommand::Get { request_id: id, key }.into());
                    receiver
                };

                match receiver.await {
                    Ok(Some((slot, value))) => Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("X-Paxos-Slot", slot)
                        .body(value.into())
                        .unwrap()),
                    Ok(None) => respond(StatusCode::NOT_FOUND),
                    Err(_) => respond(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            (_, key) if key == "paxos" => respond(StatusCode::METHOD_NOT_ALLOWED),
            _ => respond(StatusCode::NOT_FOUND),
        }
    }
}

fn respond(code: StatusCode) -> Result<Response<Body>, hyper::Error> {
    let mut resp = Response::default();
    *resp.status_mut() = code;
    Ok(resp)
}
