pub mod catalog {
    tonic::include_proto!("catalog"); // The string specified here must match the proto package name
}

pub mod replica {
    tonic::include_proto!("replica");
}

use catalog::catalog_server::{Catalog, CatalogServer};
use catalog::{AnswerReply, CreateReply, QueryRequest};
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use futures_locks::RwLock;
use tonic::{transport::Server, Code, Request, Response, Status};

#[allow(unused_imports)]
use log::{error, info, trace, warn};

use crdts::{CmRDT, CvRDT, PNCounter};
use num_traits::cast::ToPrimitive;
use replica::replica_client::ReplicaClient;
use replica::replica_server::{Replica, ReplicaServer};
use replica::{AliveRequest, ApplyRequest};
use replica::apply_request::Datatype;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tokio::{self, task, try_join};
use serde::ser::Serialize;
use core::fmt::Debug;

type CatalogValueTable = HashMap<String, PNCounter<String>>;
type PeerTable = HashMap<SocketAddr, Option<ReplicaClient<tonic::transport::Channel>>>;
#[derive(Debug)]
pub struct CroutonCatalog {
    values: RwLock<CatalogValueTable>,
    peers: RwLock<PeerTable>,
    wakeup_tx: Option<Sender<()>>,
}

impl CroutonCatalog {
    pub fn new(peers: Vec<SocketAddr>, wakeup: Option<Sender<()>>) -> CroutonCatalog {
        CroutonCatalog {
            values: RwLock::new(HashMap::new()),
            peers: RwLock::new(peers.iter().map(|s| (*s, None)).collect()),
            wakeup_tx: wakeup,
        }
    }

    pub fn get_wakeup_sender(&self) -> Sender<()> {
        let tx = self.wakeup_tx.as_ref().unwrap();
        tx.clone()
    }

    pub async fn check_connections(&self, rx: &mut Receiver<()>) {
        loop {
            let peers = self.peers.clone();

            // Why are we cloning here? Because otherwise we need to hold the
            // read lock, and cannot then grab the write lock later on.
            // So we use the read lock, grab a snapshot (we're fine if its a bit stale)
            // and use the clone of that.
            let peer = peers.read().await.clone();

            info!("CroutonCatalog::check_connections: Waking up and checking connections");

            // Find the list of peers with home we do not have connections.
            let missing_peers = peer
                .iter()
                .filter_map(|(&addr, client)| {
                    if client.as_ref().is_none() {
                        Some(addr)
                    } else {
                        None
                    }
                })
                .collect::<Vec<SocketAddr>>();

            // If there are no peers without connections, we can just end this - no more work to do!
            if missing_peers.is_empty() {
                info!("CroutonCatalog::check_connections: No update to peer clients so exiting early.");
                return;
            }

            let mut new_clients = Vec::new();
            for addr in missing_peers.iter() {
                let res = ReplicaClient::connect(format!("http://{}", addr.to_string())).await;
                let new_client = res.ok();
                info!(
                    "CroutonCatalog::check_connections: Successfully connected to {}",
                    addr
                );
                new_clients.push((addr, new_client));

                // TODO: If this is a new connection, we should send all our state to initialize it!
            }

            // Put this code in its own block to ensure we release the write lock before we wait
            // on the wakeup signal.
            // Add the results from our attempts to connect into our table.
            // Each such entry is a valid client Some(client) or invalid None.
            {
                let mut table = self.peers.write().await;
                for (&addr, client) in new_clients.iter() {
                    if let Some(c) = client {
                        table.insert(addr, Some(c.clone()));
                    } else {
                        table.insert(addr, None);
                    }
                }
                info!(
                    "CroutonCatalog::check_connections: Dump of peers: {:?}",
                    table
                        .values()
                        .filter(|v| v.is_some())
                        .collect::<Vec<_>>()
                        .len()
                );
            }

            // Wait for a wakeup signal to check all the connections again.
            rx.recv().await;
        }
    }

    async fn send_update<T: CvRDT + Debug + Serialize>(&self, name: &str, crdt: &T) {
        trace!("CroutonCatalog::send_update: name: {:?} op: {:?}", name, crdt);

        let crdt_json = &serde_json::to_string(&crdt).unwrap();

        let peers = self.peers.clone();
        trace!("CroutonCatalog::send_update: Attempt to get read lock on peers.");
        let peer = peers.read().await;
        trace!("CroutonCatalog::send_update: Obtained read lock on peers.");

        info!(
            "CroutonCatalog::send_update: Dump of peers: {:?}",
            peer.keys().map(|k| k.to_string())
        );

        let live_peers = peer
            .values()
            .filter_map(|client| client.as_ref())
            .collect::<Vec<_>>();
        info!(
            "CroutonCatalog::send_update: Send update to {} peers.",
            live_peers.len()
        );

        let changes = stream::iter(live_peers.iter())
            .map(|&client| async move {
                let msg = tonic::Request::new(ApplyRequest {
                    name: name.to_string(),
                    datatype: Datatype::Counter as i32,
                    crdt: crdt_json.clone(),
                });

                let mut c = client.clone();
                info!(
                    "CroutonCatalog::send_update: send apply for Op `{:?}` to {:?}",
                    &msg, c
                );
                let ans = c.apply(msg).await;
                trace!("CroutonCatalog::send_update: apply complete");
                ans
            })
            .collect::<Vec<_>>()
            .await;

        join_all(changes).await;
    }
}

#[tonic::async_trait]
impl Catalog for Arc<CroutonCatalog> {
    async fn create(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<CreateReply>, Status> {
        info!(
            "Catalog::create: Got a request to create counter: {:?}",
            request
        );

        let name = request.get_ref().name.clone();
        let table = self.values.read().await;
        if table.contains_key(&name) {
            return Ok(Response::new(catalog::CreateReply { status: 1 }));
        }
        // Release the read lock as now we need to get the write lock to create the entry.
        drop(table);

        let mut values = self.values.write().await;
        values.insert(name, PNCounter::new());
        let reply = catalog::CreateReply { status: 0 };
        Ok(Response::new(reply))
    }

    async fn read(&self, request: Request<QueryRequest>) -> Result<Response<AnswerReply>, Status> {
        info!("Catalog::read: Got a read request {:?}", request);

        let name = &request.get_ref().name;
        let values = self.values.read().await;
        match values.get(name) {
            None => Err(Status::new(
                Code::Unknown,
                format!("No such value to read named {}", name),
            )),
            Some(val) => {
                let reply = catalog::AnswerReply {
                    value: val.read().to_i32().unwrap(),
                };

                Ok(Response::new(reply))
            }
        }
    }

    async fn inc(&self, request: Request<QueryRequest>) -> Result<Response<AnswerReply>, Status> {
        info!("Catalog::inc: Got a inc request {:?}", request);

        let name = &request.get_ref().name;
        let actor = &request.get_ref().actor;
        let mut values = self.values.write().await;
        match values.get_mut(name) {
            None => Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                format!("No such counter: {}", name),
            )),
            Some(val) => {
                let op = val.inc(actor.clone());
                info!("Catalog::inc: Increment {} with op `{:?}`.", name, op);
                val.apply(op.clone());

                // TODO: Change this to send actual value rather than an "op"
                info!("Catalog::inc: Send update to peers: {} {:?}", name, op);
                self.send_update(name, val).await;
                info!("Catalog::inc: Updates sent to all peers: {} {:?}", name, op);

                Ok(Response::new(catalog::AnswerReply {
                    value: val.read().to_i32().unwrap(),
                }))
            }
        }
    }
}

#[tonic::async_trait]
impl Replica for Arc<CroutonCatalog> {
    async fn apply(&self, apply: Request<ApplyRequest>) -> Result<Response<()>, Status> {
        trace!("apply: msg {:?}", apply);

        let name = &apply.get_ref().name;
        let msg = &apply.get_ref().crdt;
        // TODO: Will need to encode the type in the message and deserialize accordingly
        // once we support more than PNCounter.
        let crdt: PNCounter<String> = serde_json::from_str(msg).unwrap();
        let mut values = self.values.write().await;

        // Likely should do something if I haven't seen this value before.
        if let Some(val) = values.get_mut(name) {
            info!("Replica::apply: Val was {:?} with op {:?}", val, &crdt);
            val.merge(crdt);
            info!("Replica::apply now {:?}", val);
        } else {
            info!(
                "Replica::apply: New value seen {:?}, applying op {:?}",
                name, &crdt
            );
            let mut new_val = PNCounter::new();
            new_val.merge(crdt);
            values.insert(name.clone(), new_val);
        }

        Ok(Response::new(()))
    }

    async fn alive(&self, _req: Request<AliveRequest>) -> Result<Response<()>, Status> {
        let tx = self.get_wakeup_sender();
        tx.send(()).await.unwrap();
        Ok(Response::new(()))
    }
}

pub async fn build_services(
    cluster: Vec<SocketAddr>,
    c_addr: SocketAddr,
    r_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("build_services: Starting server...");

    let (tx, mut rx): (Sender<()>, Receiver<()>) = mpsc::channel(100);
    let catalog = Arc::new(CroutonCatalog::new(cluster, Some(tx)));
    let forever = catalog.clone();
    task::spawn(async move {
        let x = forever.clone();
        x.check_connections(&mut rx).await;
    });

    let tx = catalog.get_wakeup_sender();
    task::spawn(async move {
        sleep(Duration::from_millis(10000)).await;
        tx.send(()).await.unwrap();
    });

    let catalog_server = Server::builder()
        .add_service(CatalogServer::new(catalog.clone()))
        .serve(c_addr);
    info!(
        "build_services: Starting up Catalog Service on port {}.",
        c_addr.to_string()
    );

    let replica_server = Server::builder()
        .add_service(ReplicaServer::new(catalog.clone()))
        .serve(r_addr);
    info!(
        "build_services: Starting up Replica Service on port {}.",
        r_addr.to_string()
    );
    try_join!(catalog_server, replica_server)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn init_logger() {
        INIT.call_once(pretty_env_logger::init);
    }

    #[tokio::test]
    async fn simple_create() {
        init_logger();

        let catalog = Arc::new(CroutonCatalog::new(Vec::new(), None));

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await.unwrap();
        assert_eq!(response.get_ref().status, 0);
    }

    #[tokio::test]
    async fn simple_create_and_inc() {
        init_logger();

        let catalog = Arc::new(CroutonCatalog::new(Vec::new(), None));

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await.unwrap();
        assert_eq!(response.get_ref().status, 0);

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.inc(request).await.unwrap();
        assert_eq!(response.get_ref().value, 1);
    }

    #[tokio::test]
    async fn inc_three() {
        init_logger();

        let catalog = Arc::new(CroutonCatalog::new(Vec::new(), None));

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await.unwrap();
        assert_eq!(response.get_ref().status, 0);

        let mut response = None;
        for _ in 0..3 {
            let request = tonic::Request::new(QueryRequest {
                name: "VoteCounter".into(),
                actor: "Me".to_string(),
            });

            response = Some(catalog.inc(request).await.unwrap());
        }
        assert_eq!(response.unwrap().get_ref().value, 3);
    }

    #[tokio::test]
    async fn inc_ten_and_read() {
        init_logger();

        let catalog = Arc::new(CroutonCatalog::new(Vec::new(), None));

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await.unwrap();
        assert_eq!(response.get_ref().status, 0);

        for _ in 0..10 {
            let request = tonic::Request::new(QueryRequest {
                name: "VoteCounter".into(),
                actor: "Me".to_string(),
            });

            catalog.inc(request).await.unwrap();
        }

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.read(request).await.unwrap();
        assert_eq!(response.get_ref().value, 10);
    }

    #[tokio::test]
    async fn do_apply() {
        init_logger();

        let original = Arc::new(CroutonCatalog::new(Vec::new(), None));
        let target = Arc::new(CroutonCatalog::new(Vec::new(), None));

        let name = "VoteCounter";
        let actor = "Me";

        let request = tonic::Request::new(QueryRequest {
            name: name.into(),
            actor: actor.to_string(),
        });

        original.create(request).await.unwrap();

        // This is a bit ugly but I'm hacking into the internals of MyCatalog, and
        // replicating the logic of inc. Its the only way to get the Op built.
        let mut values = original.values.write().await;
        let val = values.get_mut(name).unwrap();

        for _ in 0..3 {
            let op = val.inc(actor.to_string());
            val.apply(op);
            let request = tonic::Request::new(ApplyRequest {
                name: "VoteCounter".into(),
                datatype: Datatype::Counter as i32,
                crdt: serde_json::to_string(&val).unwrap(),
            });

            target.apply(request).await.unwrap();
        }

        // Now see if that worked by reading the value in 'target'
        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = target.read(request).await.unwrap();
        assert_eq!(response.get_ref().value, 3);
    }
}
