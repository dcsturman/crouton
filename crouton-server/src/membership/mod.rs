// These interfaces should define a membership service that can run underneath the Crouton server
// but at the same time be completely independent of it - no crouton-specific behavior should fall
// to this layer.  Instead this service should:
// 0. This libary should express an interface (trait) so the user of the library can use different implementations
//    without any code change except initialization.
// 1. Maintain a list of live peers and keep it up to date.
// 2. Discover new peers and via an interface get them updated.  Part of the magic should be this library behind the scenes
//    is finding peers and growing the set with minimum work.
// 3. The user of the libary should be able to signal loss of a peer (say due to an error)
// 4. The user of the library should give an interface to initialize a peer.
// 5. The user of the library should give an interface to attempt to connect to a peer, and return a success or failure.
use anyhow::Result;
use async_trait::async_trait;
use futures_locks::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver};

#[allow(unused_imports)]
use log::{error, info, trace, warn};

#[async_trait]
pub trait MembershipUpcall<T: Sync + Send> {
    async fn initialize_new_client(&self, addr: &SocketAddr) -> Result<T>;
}

#[async_trait]
pub trait MembershipService<T: Clone + Sync + Send> {
    async fn get_peers(&self) -> Vec<(SocketAddr, Option<T>)>;
    async fn remove_peer(&self, addr: &SocketAddr);
    async fn add_peer(&self, addr: &SocketAddr);
    async fn wakeup(&self);
    async fn check_connections(
        &self,
        handler: Arc<(dyn MembershipUpcall<T> + Sync + Send)>,
        rx: &mut Receiver<()>,
    );
}

pub struct SimpleMembershipService<T: Clone + Sync + Send> {
    peers: Arc<RwLock<HashMap<SocketAddr, Option<T>>>>,
    tx: Option<mpsc::Sender<()>>,
}

impl<'a, T: Clone + Sync + Send> SimpleMembershipService<T> {
    pub fn new(peers: Vec<SocketAddr>) -> SimpleMembershipService<T> {
        info!("Creating membership service with peers: {:?}", peers);
        let peer_table = peers.iter().map(|peer| (*peer, None)).collect();

        SimpleMembershipService {
            peers: Arc::new(RwLock::new(peer_table)),
            tx: None,
        }
    }
}

#[async_trait]
impl<'a, T: Clone + Sync + Send> MembershipService<T> for SimpleMembershipService<T> {
    async fn get_peers(&self) -> Vec<(SocketAddr, Option<T>)> {
        self.peers
            .read()
            .await
            .iter()
            .map(|(addr, client)| (*addr, client.clone()))
            .collect()
    }

    async fn remove_peer(&self, addr: &SocketAddr) {
        self.peers.write().await.insert(*addr, None);
    }

    async fn add_peer(&self, addr: &SocketAddr) {
        let has_key = self.peers.read().await.contains_key(addr);
        if has_key {
            self.peers.write().await.insert(*addr, None);
        }
    }

    async fn wakeup(&self) {
        self.tx.as_ref().unwrap().send(()).await.unwrap();
    }

    async fn check_connections(
        &self,
        handler: Arc<(dyn MembershipUpcall<T> + Sync + Send)>,
        rx: &mut Receiver<()>,
    ) {
        loop {
            // Wait for a wakeup signal to check all the connections again.
            rx.recv().await;

            // Why are we cloning here? Because otherwise we need to hold the
            // read lock, and cannot then grab the write lock later on.
            // So we use the read lock, grab a snapshot (we're fine if its a bit stale)
            // and use the clone of that.
            let peers = self.peers.read().await.clone();

            info!("SimpleMembershipService::check_connections: Waking up and checking connections");

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
                info!("SimpleMembershipService::check_connections: No update to peer clients so exiting early.");
                continue;
            }

            let mut new_clients = Vec::new();
            for addr in missing_peers.iter() {
                info!(
                    "SimpleMembershipService::check_connections: Trying address {:?}",
                    addr
                );
                let new_client = handler.initialize_new_client(addr).await.ok();
                if new_client.is_none() {
                    info!(
                        "SimpleMembershipService::check_connections: Didn't connect to {:?}",
                        addr
                    );
                } else {
                    info!("SimpleMembershipService::check_connections: Successfully connected to {:?}", addr);
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
                        table.insert(addr, Some((*c).clone()));
                    } else {
                        table.insert(addr, None);
                    }
                }
            }
        }
    }
}

// Implementation for MembershipService for use in things like unit tests.
// It does nothing in for unit tests most of the methods should never get called.
pub struct DummyMembershipService;

#[cfg(test)]
impl DummyMembershipService {
    pub fn new() -> DummyMembershipService {
        DummyMembershipService {}
    }
}

#[async_trait]
impl<T: 'static + Clone + Sync + Send> MembershipService<T> for DummyMembershipService {
    async fn get_peers(&self) -> Vec<(SocketAddr, Option<T>)> {
        Vec::new()
    }

    async fn remove_peer(&self, _addr: &SocketAddr) {
        unimplemented!("Dummy membership should never be called.");
    }

    async fn add_peer(&self, _addr: &SocketAddr) {
        unimplemented!("Dummy membership should never be called.");
    }

    async fn wakeup(&self) {
        unimplemented!("Dummy membership should never be called.");
    }

    async fn check_connections(
        &self,
        _handler: Arc<(dyn MembershipUpcall<T> + Sync + Send)>,
        _rx: &mut Receiver<()>,
    ) {
        unimplemented!("Dummy membership should never be called.");
    }
}
