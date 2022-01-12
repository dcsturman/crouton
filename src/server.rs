use tonic::{transport::Server, Request, Response, Status};

use clap::{App, Arg};
use crouton::catalog_server::{Catalog, CatalogServer};
use crouton::{AnswerReply, CreateReply, QueryRequest, ApplyRequest};
use num_traits::cast::ToPrimitive;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::net::SocketAddr;

use crdts::{CmRDT, PNCounter};

pub mod crouton {
    tonic::include_proto!("crouton"); // The string specified here must match the proto package name
}

#[derive(Debug)]
pub struct MyCatalog {
    values: Arc<RwLock<HashMap<String, PNCounter<String>>>>,
    //TODO: Ensure this type is right: CatalogServer<MyCatalog>
    peers: Arc<RwLock<HashMap<SocketAddr, Option<CatalogServer<MyCatalog>>>>>,
}

impl MyCatalog {
    fn new(peers: Vec<SocketAddr>) -> MyCatalog {
        MyCatalog { 
            values: Arc::new(RwLock::new(HashMap::new())), 
            peers: Arc::new( RwLock::new(peers.iter().map(|s| (*s, None)).collect())),
        }
    }

    async fn check_connections(&self) {
        for (server, connected) in self.peers.write().unwrap().iter() {
            if  !connected {
                let client = 
            }

        }
    }
}

#[tonic::async_trait]
impl Catalog for MyCatalog {
    async fn create(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<CreateReply>, Status> {
        println!("Got a request to create counter: {:?}", request);

        let name = request.get_ref().name.clone();
        let reply = if self.values.read().unwrap().contains_key(&name) {
            crouton::CreateReply { status: 1 }
        } else {
            self.values.write().unwrap().insert(name, PNCounter::new());
            crouton::CreateReply { status: 0 }
        };

        Ok(Response::new(reply))
    }

    async fn read(&self, request: Request<QueryRequest>) -> Result<Response<AnswerReply>, Status> {
        println!("Got a read request {:?}", request);

        let name = &request.get_ref().name;
        let values = self.values.read().unwrap();
        let val = values.get(name).unwrap();
        let reply = crouton::AnswerReply {
            value: val.read().to_i32().unwrap(),
        };
        Ok(Response::new(reply))
    }

    async fn inc(&self, request: Request<QueryRequest>) -> Result<Response<AnswerReply>, Status> {
        println!("Got a inc request {:?}", request);

        let name = &request.get_ref().name;
        let actor = &request.get_ref().actor;
        let mut values = self.values.write().unwrap();
        match values.get_mut(name) {
            None => Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                format!("No such counter: {}", name),
            )),
            Some(val) => {
                let op = val.inc(actor.clone());
                val.apply(op);
                Ok(Response::new(crouton::AnswerReply {
                    value: val.read().to_i32().unwrap(),
                }))
            }
        }
    }

    async fn apply(&self, apply: Request<ApplyRequest>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("Crouton Server")
        .version("0.1")
        .author("Daniel Sturman <dan@sturman.org>")
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("Port for this server to listen on.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("servers")
                .short("s")
                .long("servers")
                .help("List of IP:PORT pairs to connect to peers in this cluster.")
                .multiple(true),
        )
        .get_matches();

    let port = matches.value_of("port").unwrap_or("50051");
    let addr = format!("[::1]:{}", port).parse()?;

    let cluster = match matches.values_of("servers") {
        None => Vec::new(),
        Some(servers) => servers.map(|s| s.parse::<SocketAddr>().unwrap()).collect::<Vec<SocketAddr>>()
    };

    let catalog = MyCatalog::new(cluster);

    Server::builder()
        .add_service(CatalogServer::new(catalog))
        .serve(addr)
        .await?;

    Ok(())
}
