use anyhow::Result;
use async_trait::async_trait;
use crouton_protos::catalog::catalog_server::{Catalog, CatalogServer};
use crouton_protos::catalog::{AnswerReply, QueryRequest};
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use futures_locks::RwLock;
use tonic::{transport::Channel, transport::Server, Code, Request, Response, Status};

#[allow(unused_imports)]
use log::{error, info, trace, warn};

use crdts::{CmRDT, CvRDT, PNCounter};
use crouton_protos::replica::replica_server::{Replica, ReplicaServer};
use crouton_protos::replica::{
    apply_request::Datatype, replica_client::ReplicaClient, AliveRequest, ApplyRequest,
};
use num_traits::cast::ToPrimitive;

use core::fmt::Debug;
use serde::ser::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio::{self, task, try_join};

pub mod membership;

use crate::membership::simple::SimpleMembershipService;
use crate::membership::{MembershipService, MembershipUpcall};

type CatalogValueTable = HashMap<String, PNCounter<String>>;

/// Core struct for the entire service. `CroutonCatalog` maintains a set of CRDTs that are
/// all shared between a set of peers.  Thanks to the properties of CRDTs they'll eventually converge.
/// The `CroutonCatalog` implements interfaces to
/// 1. Be called by a memebership service when it has as new peer to initialize.
/// 2. Implement the Crouton grpc service to accept requests from clients.
/// 3. Implement the Replica grpc service to accept requests from peers.
pub struct CroutonCatalog {
    /// IP address for this server, used with grpc interfaces.
    address: SocketAddr,
    /// A hashmap from names of crdt's to crdts. Today all crdt's are counters (PNCounter).  Names
    /// must be globally (across all servers) unique.
    values: RwLock<CatalogValueTable>,
    /// Reference to a membership service, which can be implemented by any of a number of ways.
    /// The `CroutonCatalog` doesn't care what the implemetnation is.
    membership: Box<dyn MembershipService<ReplicaClient<Channel>> + Send + Sync>,
}

#[async_trait]
impl<'a> MembershipUpcall<ReplicaClient<Channel>> for CroutonCatalog {
    async fn initialize_new_connection(
        &self,
        addr: &SocketAddr,
    ) -> Result<ReplicaClient<tonic::transport::Channel>> {
        info!(
            "CroutonCatalog::initialize_new_connection: {:?} initializing new connection to {:?}",
            self.address, addr
        );
        let mut new_client = ReplicaClient::connect(format!("http://{}", addr)).await?;

        let values = self.values.read().await;
        info!(
            "CroutonCatalog::initialize_new_connection: {:?} sending all state to {:?}",
            self.address, addr
        );
        CroutonCatalog::send_all_state(&mut new_client, &values).await?;

        let msg = AliveRequest {
            address: self.address.to_string(),
        };

        info!(
            "CroutonCatalog::initialize_new_connection: {:?} sending alive message to {:?}",
            self.address, addr
        );
        new_client.alive(msg).await?;

        Ok(new_client)
    }
}

impl CroutonCatalog {
    pub fn new(
        address: SocketAddr,
        membership: Box<dyn MembershipService<ReplicaClient<Channel>> + Send + Sync>,
    ) -> CroutonCatalog {
        CroutonCatalog {
            address,
            values: RwLock::new(HashMap::new()),
            membership,
        }
    }

    async fn send_all_state(
        client: &mut ReplicaClient<Channel>,
        values: &CatalogValueTable,
    ) -> Result<Response<()>, Status> {
        info!(
            "CroutonCatalog::send_all_state: Sending all state {:?}",
            values
        );
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
        info!("CroutonCatalog::remove_peer: Removing {:?}", addr);
        self.membership.remove_peer(addr).await;
    }

    async fn send_update<T: CvRDT + Debug + Serialize>(&self, name: &str, crdt: &T) {
        trace!(
            "CroutonCatalog::send_update: name: {:?} op: {:?}",
            name,
            crdt
        );

        let crdt_json = &serde_json::to_string(&crdt).unwrap();

        let peers = self.membership.get_peers().await;

        info!(
            "CroutonCatalog::send_update: Dump of peers: {:?}",
            peers.iter().map(|(k, _)| k.to_string())
        );

        let live_peers = peers
            .iter()
            .filter(|(_, client)| client.is_some())
            .map(|(addr, client)| (addr, client.as_ref().unwrap().clone()))
            .collect::<Vec<_>>();
        info!(
            "CroutonCatalog::send_update: Send update to {} peers.",
            live_peers.len()
        );

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

    async fn check_connections(
        &self,
        handler: Arc<dyn MembershipUpcall<ReplicaClient<Channel>> + Send + Sync + 'static>,
    ) {
        self.membership.check_connections(handler).await;
    }
}

/// A simple wrapper for an `Arc<CroutonCatalog>`.  We provide this wrapper so that we can implement
/// our two compiled grpc struture for this type.  If we just used `Arc<CroutonCatalog>` we could
/// not do `impl Catalog for Arc<CroutonCatalog> {...}`.
///
/// We implement some helpful traits (Deref, DerefMut, Clone, From) to make things easier.
pub struct ArcCroutonCatalog(Arc<CroutonCatalog>);

impl ArcCroutonCatalog {
    pub fn new(catalog: CroutonCatalog) -> ArcCroutonCatalog {
        ArcCroutonCatalog(Arc::new(catalog))
    }
}

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

impl From<ArcCroutonCatalog> for Arc<CroutonCatalog> {
    fn from(acc: ArcCroutonCatalog) -> Arc<CroutonCatalog> {
        acc.0
    }
}

/// Implements the grpc Crouton interface for our underlying CroutonCatalog struct.  However, we do
/// this for an ArcCroutonCatalog (really an Arc<CroutonCatalog>) as that is how its passed to the tonic logic.
/// Crouton is the grpc interface a client uses to talk to a Crouton server serving shared crdt's.
#[tonic::async_trait]
impl Catalog for ArcCroutonCatalog {
    /// Create a new crdt.  The request takes the following proto - net a name for the crdt and the actor creating it.
    /// ```ignore
    /// message QueryRequest {
    ///   string name = 1;
    ///   string actor = 2;
    /// }
    /// ```
    /// Right now all new crdt's are counters and we'll change that in the future.
    /// Note we could have skipped this verb and just had an operation auto-create a crdt but this helps
    /// us avoid typo's where you could be working on the wrong counters in a distributed environment.
    async fn create(&self, request: Request<QueryRequest>) -> Result<Response<()>, Status> {
        info!(
            "Catalog::create: Got a request to create counter: {:?}",
            request
        );

        let name = request.get_ref().name.clone();
        let table = self.values.read().await;
        if table.contains_key(&name) {
            return Err(Status::new(
                Code::AlreadyExists,
                format!("Key {} already exists.", &name),
            ));
        }

        // Release the read lock as now we need to get the write lock to create the entry.
        drop(table);

        let mut values = self.values.write().await;
        values.insert(name, PNCounter::new());
        Ok(Response::new(()))
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
        self.membership.wakeup().await;
        info!("Replica::alive: {:?} woke itself up.", self.address);
        Ok(Response::new(()))
    }
}

pub async fn build_services(
    cluster: Vec<SocketAddr>,
    c_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("build_services: Starting server...");

    // We need some sort of communcation between the membership service and the catalog.  I address this by having the catalog know about
    // the membership service (we pass in the Box<SimpleMembershipService> and the Catalog owns this. In this way the catalog can query
    // things like the current list of peers.
    // To connect in the other direction (membershipservice->catalog) we note the only place membership needs to call the catalog is
    // in a call to the MembershipUpcall trait, and that is done exclusively in check_connections.  So we call that from the catalog's
    // check_connections, with a parameter that is the catalog (but acting as the MembershipUpcall).  Its all a bit messy but
    // its safe and it works.
    let membership_service = Box::new(SimpleMembershipService::new(cluster));
    let catalog = ArcCroutonCatalog::new(CroutonCatalog::new(c_addr, membership_service));

    let forever = catalog.clone();
    task::spawn(async move {
        // OMG this is ugly. I'm passing x in as the struct (self) and as another pointer.  This is necessary
        // as we the implementation of MembershipUpcall to be an Arc<_> but the catalog owns the reference to MembershipService.
        forever.check_connections(forever.clone().0).await;
    });

    task::yield_now().await;
    let forever = catalog.clone();
    task::spawn(async move {
        loop {
            forever.membership.wakeup().await;
            sleep(Duration::from_millis(10000)).await;
        }
    });

    task::yield_now().await;

    let catalog_server = Server::builder()
        .add_service(CatalogServer::new(catalog.clone()))
        .add_service(ReplicaServer::new(catalog.clone()))
        .serve(c_addr);
    info!(
        "build_services: Starting up Catalog Service on port {}.",
        c_addr
    );
    try_join!(catalog_server)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use membership::dummy::DummyMembershipService;

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
            Box::new(DummyMembershipService::new()),
        )));

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn double_create_fail() {
        init_logger();

        let catalog = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(
            DUMMY_ADDR.parse().unwrap(),
            Box::new(DummyMembershipService::new()),
        )));

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await;
        assert!(response.is_ok());

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await;
        assert!(response.is_err());
    }

    #[tokio::test]
    async fn simple_create_and_inc() {
        init_logger();

        let catalog = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(
            DUMMY_ADDR.parse().unwrap(),
            Box::new(DummyMembershipService::new()),
        )));

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await;
        assert!(response.is_ok());

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
            Box::new(DummyMembershipService::new()),
        )));

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await;
        assert!(response.is_ok());

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
            Box::new(DummyMembershipService::new()),
        )));

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = catalog.create(request).await;
        assert!(response.is_ok());

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
            Box::new(DummyMembershipService::new()),
        )));

        let target = ArcCroutonCatalog(Arc::new(CroutonCatalog::new(
            DUMMY_ADDR.parse().unwrap(),
            Box::new(DummyMembershipService::new()),
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
