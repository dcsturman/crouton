use crate::membership::{MembershipService, MembershipUpcall};
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::sync::Mutex;

#[allow(unused_imports)]
use log::{error, info, trace, warn};

pub struct SimpleMembershipService<T: Clone + Sync + Send> {
    peers: Arc<RwLock<HashMap<SocketAddr, Option<T>>>>,
    tx: mpsc::Sender<()>,
    rx: Mutex<mpsc::Receiver<()>>,
}

impl<'a, T: Clone + Sync + Send> SimpleMembershipService<T> {
    pub fn new(peers: Vec<SocketAddr>) -> SimpleMembershipService<T> {
        info!("Creating membership service with peers: {:?}", peers);
        let peer_table = peers.iter().map(|peer| (*peer, None)).collect();
        let (tx, rx) = mpsc::channel(100);

        SimpleMembershipService {
            peers: Arc::new(RwLock::new(peer_table)),
            tx,
            rx: Mutex::new(rx),
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
        self.tx.send(()).await.unwrap();
    }

    async fn check_connections(&self, handler: Arc<(dyn MembershipUpcall<T> + Sync + Send)>) {
        // This is a bit of a hack that I'll explain here.
        // In theory there could be many check_connection calls or threads, but there will only be one in reality.
        // However, the call path is through an owner of this membership object, and likely that owner is an Arc<_>.
        // So, the mutex allows us to get a mut on the receiver, but grabbing that lock up front is "safe"
        // as there will only ever be one call here and one user of the rx.
        // TODO: Is there a better way to do this?
        let mut rx = self.rx.lock().await;
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
                let new_client = handler.initialize_new_connection(addr).await.ok();
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
