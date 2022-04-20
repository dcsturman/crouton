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
use std::net::SocketAddr;
use std::sync::Arc;

pub mod dummy;
pub mod simple;

#[allow(unused_imports)]
use log::{error, info, trace, warn};

#[async_trait]
pub trait MembershipUpcall<T: Sync + Send> {
    async fn initialize_new_connection(&self, addr: &SocketAddr) -> Result<T>;
}

#[async_trait]
pub trait MembershipService<T: Clone + Sync + Send> {
    async fn get_peers(&self) -> Vec<(SocketAddr, Option<T>)>;
    async fn remove_peer(&self, addr: &SocketAddr);
    async fn add_peer(&self, addr: &SocketAddr);
    async fn wakeup(&self);
    async fn check_connections(&self, handler: Arc<(dyn MembershipUpcall<T> + Sync + Send)>);
}
