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
