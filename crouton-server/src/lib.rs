
use crouton_protos::catalog::catalog_server::{Catalog, CatalogServer};
use crouton_protos::catalog::{AnswerReply, CreateReply, QueryRequest};
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use futures_locks::RwLock;
use tonic::{transport::Channel, transport::Server, Code, Request, Response, Status};

#[allow(unused_imports)]
use log::{error, info, trace, warn};

use crdts::{CmRDT, CvRDT, PNCounter};
use num_traits::cast::ToPrimitive;
use crouton_protos::replica::{ AliveRequest, ApplyRequest, apply_request::Datatype, replica_client::ReplicaClient };
use crouton_protos::replica::replica_server::{Replica, ReplicaServer};

use core::fmt::Debug;
use serde::ser::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::ops::{Deref, DerefMut};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tokio::{self, task, try_join};

type CatalogValueTable = HashMap<String, PNCounter<String>>;
type PeerTable = HashMap<SocketAddr, Option<ReplicaClient<tonic::transport::Channel>>>;
#[derive(Debug)]
pub struct CroutonCatalog {
    address: SocketAddr,
    values: RwLock<CatalogValueTable>,
    peers: RwLock<PeerTable>,
    wakeup_tx: Option<Sender<()>>,
}

impl CroutonCatalog {
    pub fn new(
        address: SocketAddr,
        peers: Vec<SocketAddr>,
        wakeup: Option<Sender<()>>,
    ) -> CroutonCatalog {
        CroutonCatalog {
            address,
            values: RwLock::new(HashMap::new()),
            peers: RwLock::new(peers.iter().map(|s| (*s, None)).collect()),
            wakeup_tx: wakeup,
        }
    }

    pub fn get_wakeup_sender(&self) -> Sender<()> {
        let tx = self.wakeup_tx.as_ref().unwrap();
        tx.clone()
    }

    async fn send_all_state(
        mut client: ReplicaClient<Channel>,
        values: &CatalogValueTable,
    ) -> Result<Response<()>, Status> {
        for (name, value) in values.iter() {
            let msg = ApplyRequest {
                name: name.clone(),
                datatype: Datatype::Counter as i32,
                crdt: serde_json::to_string(value).unwrap(),
            };

            client.apply(msg).await?;
        }
        Ok(Response::new(()))
    }

    async fn remove_peer(&self, addr: &SocketAddr) {
        let mut peers = self.peers.write().await;
        info!("CroutonCatalog::remove_peer: Removing {:?}", addr);
        peers.insert(*addr, None);
    }

    pub async fn check_connections(&self, rx: &mut Receiver<()>) {
        loop {
            // Wait for a wakeup signal to check all the connections again.
            rx.recv().await;

            // Why are we cloning here? Because otherwise we need to hold the
            // read lock, and cannot then grab the write lock later on.
            // So we use the read lock, grab a snapshot (we're fine if its a bit stale)
            // and use the clone of that.
            let peers = self.peers.read().await.clone();

            info!("CroutonCatalog::check_connections: Waking up and checking connections");

            // Find the list of peers with home we do not have connections.
            let missing_peers = peers
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
                continue;
            }

            let values = self.values.read().await.clone();
            let mut new_clients = Vec::new();
            for addr in missing_peers.iter() {
                let new_client = ReplicaClient::connect(format!("http://{}", addr))
                    .await
                    .ok();

                if let Some(active) = new_client.as_ref() {
                    let msg = AliveRequest {
                        address: self.address.to_string(),
                    };
                    let mut active = active.clone();
                    info!(
                        "CroutonCatalog::check_connections: Send alive message from {:?} to {:?}",
                        self.address.to_string(),
                        addr.to_string()
                    );
                    active.alive(msg).await.unwrap_or_else(|e| {
                        error!(
                            "CroutonCatalog::check_connections: Call to alive from {:?} to  {:?} failed: {:?}",
                            self.address,
                            addr,
                            e
                        );
                        Response::new(())
                    });
                    match CroutonCatalog::send_all_state(active, &values).await {
                        Ok(_) => (),
                        Err(_) => self.remove_peer(addr).await,
                    }
                } else {
                    info!(
                        "CroutonCatalog::check_connections: Failed to connect to {}",
                        addr
                    );
                }
                new_clients.push((addr, new_client));
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
            }
        }
    }

    async fn send_update<T: CvRDT + Debug + Serialize>(&self, name: &str, crdt: &T) {
        trace!(
            "CroutonCatalog::send_update: name: {:?} op: {:?}",
            name,
            crdt
        );

        let crdt_json = &serde_json::to_string(&crdt).unwrap();

        trace!("CroutonCatalog::send_update: Attempt to get read lock on peers.");
        let peers = self.peers.read().await;
        trace!("CroutonCatalog::send_update: Obtained read lock on peers.");

        info!(
            "CroutonCatalog::send_update: Dump of peers: {:?}",
            peers.keys().map(|k| k.to_string())
        );

        let live_peers = peers
            .iter()
            .filter(|(_, client)| client.is_some())
            .map(|(&addr, client)| (addr, client.as_ref().unwrap().clone()))
            .collect::<Vec<_>>();
        info!(
            "CroutonCatalog::send_update: Send update to {} peers.",
            live_peers.len()
        );

        drop(peers);

        let changes = stream::iter(live_peers.iter())
            .map(|(addr, client)| async move {
                let msg = tonic::Request::new(ApplyRequest {
                    name: name.to_string(),
                    datatype: Datatype::Counter as i32,
                    crdt: crdt_json.clone(),
                });

                let mut c = client.clone();
                info!(
                    "CroutonCatalog::send_update: send apply for `{:?}` to {:?}",
                    &msg, c
                );
                let ans_result = c.apply(msg).await;
                let ans = match ans_result {
                    Ok(ans) => ans,
                    Err(_) => {
                        info!("CroutonCatalog::send_update: Lost connection to {}", addr);
                        self.remove_peer(addr).await;
                        Response::new(())
                    }
                };
                trace!("CroutonCatalog::send_update: apply complete");
                ans
            })
            .collect::<Vec<_>>()
            .await;

        join_all(changes).await;
    }
}

struct ArcCroutonCatalog(Arc<CroutonCatalog>);

impl Deref for ArcCroutonCatalog {
    type Target = Arc<CroutonCatalog>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ArcCroutonCatalog {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Clone for ArcCroutonCatalog {
    fn clone(&self) -> Self {
        ArcCroutonCatalog(self.0.clone())
    }
}

#[tonic::async_trait]
impl Catalog for ArcCroutonCatalog {
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
            return Ok(Response::new(CreateReply { status: 1 }));
        }
        // Release the read lock as now we need to get the write lock to create the entry.
        drop(table);

        let mut values = self.values.write().await;
        values.insert(name, PNCounter::new());
        let reply = CreateReply { status: 0 };
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
                let reply = AnswerReply {
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

                let response = Ok(Response::new(AnswerReply {
                    value: val.read().to_i32().unwrap(),
                }));

                // Cloning val so that we can release the write lock later.  The clone
                // will only be used to send an update to peers.
                let val = val.clone();

                // Release the write lock before sending updates so we don't hold it that long.
                drop(values);

                self.send_update(name, &val).await;
                info!(
                    "Catalog::inc: Updates sent to all peers: {} {:?}",
                    name, val
                );
                response
            }
        }
    }
}

#[tonic::async_trait]
impl Replica for ArcCroutonCatalog {
    async fn apply(&self, apply: Request<ApplyRequest>) -> Result<Response<()>, Status> {
        trace!("apply: msg {:?}", apply);

        let name = &apply.get_ref().name;
        let datatype = &apply.get_ref().datatype;
        let msg = &apply.get_ref().crdt;

        // TODO: Should use Datatype.Counter instead of 0 but its not working
        let crdt = match datatype {
            0 => serde_json::from_str(msg).unwrap(),
            _ => unimplemented!("Unknown datatype in apply message."),
        };

        let mut values = self.values.write().await;

        if let Some(val) = values.get_mut(name) {
            // Merge the receveived value with the value we have for this crdt
            info!("Replica::apply: Val was {:?} with op {:?}", val, &crdt);
            val.merge(crdt);
            info!("Replica::apply now {:?}", val);
        } else {
            // We haven't seen this crdt before so create it with this value.
            info!(
                "Replica::apply: New value seen {:?}, applying op {:?}",
                name, &crdt
            );
            values.insert(name.clone(), crdt);
        }

        Ok(Response::new(()))
    }

    async fn alive(&self, req: Request<AliveRequest>) -> Result<Response<()>, Status> {
        info!(
            "Replica::alive: {:?} got alive call from {:?}",
            self.address,
            req.get_ref().address
        );
        let tx = self.get_wakeup_sender();
        tx.send(()).await.unwrap_or_else(|e| {
            panic!(
                "Replica::alive: Failed signal wakeup at {:?}: \t{:?}",
                self.0.address, e
            )
        });
        info!("Replica::alive: {:?} woke itself up.", self.0.address);
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
    let catalog = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(c_addr, cluster, Some(tx))));
    let forever = catalog.clone();
    task::spawn(async move {
        let x = forever.clone();
        x.0.check_connections(&mut rx).await;
    });

    task::yield_now().await;

    let tx = catalog.0.get_wakeup_sender();
    task::spawn(async move {
        loop {
            tx.send(()).await.unwrap();
            sleep(Duration::from_millis(10000)).await;
        }
    });

    task::yield_now().await;

    let catalog_server = Server::builder()
        .add_service(CatalogServer::new(catalog.clone()))
        .serve(c_addr);
    info!(
        "build_services: Starting up Catalog Service on port {}.",
        c_addr
    );

    let replica_server = Server::builder()
        .add_service(ReplicaServer::new(catalog.clone()))
        .serve(r_addr);
    info!(
        "build_services: Starting up Replica Service on port {}.",
        r_addr
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

    const DUMMY_ADDR: &str = "127.0.0.1:8080";

    #[tokio::test]
    async fn simple_create() {
        init_logger();

        let catalog = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(
            DUMMY_ADDR.parse().unwrap(),
            Vec::new(),
            None,
        )));

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

        let catalog = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(
            DUMMY_ADDR.parse().unwrap(),
            Vec::new(),
            None,
        )));

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

        let catalog = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(
            DUMMY_ADDR.parse().unwrap(),
            Vec::new(),
            None,
        )));

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

        let catalog = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(
            DUMMY_ADDR.parse().unwrap(),
            Vec::new(),
            None,
        )));

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

        let original = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(
            DUMMY_ADDR.parse().unwrap(),
            Vec::new(),
            None,
        )));

        let target = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(
            DUMMY_ADDR.parse().unwrap(),
            Vec::new(),
            None,
        )));

        let name = "VoteCounter";
        let actor = "Me";

        let request = tonic::Request::new(QueryRequest {
            name: name.into(),
            actor: actor.to_string(),
        });

        original.create(request).await.unwrap();

        // This is a bit ugly but I'm hacking into the internals of MyCatalog, and
        // replicating the logic of inc. Its the only way to get the Op built.
        let mut values = original.0.values.write().await;
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
