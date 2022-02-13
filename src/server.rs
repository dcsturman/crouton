extern crate crouton;

use clap::{App, Arg};

#[allow(unused_imports)]
use log::{error, info, trace, warn};


use std::net::SocketAddr;

use crouton::{build_services};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

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
                .takes_value(true)
                .multiple(true),
        )
        .get_matches();

    let c_port = matches.value_of("port").unwrap_or("50051");
    let r_port = (c_port.parse::<i32>().unwrap_or(50052) + 1).to_string();
    let c_addr = format!("[::1]:{}", c_port).parse()?;
    let r_addr = format!("[::1]:{}", r_port).parse()?;

    let cluster = match matches.values_of("servers") {
        None => Vec::new(),
        Some(servers) => servers
            .map(|s| {
                s.parse::<SocketAddr>()
                    .unwrap_or_else(|_| panic!("Server address {:?} is unparseable.", s))
            })
            .collect::<Vec<SocketAddr>>(),
    };

    info!("Known peers: {:?}", cluster);

    build_services(cluster, c_addr, r_addr).await?;

    Ok(())
}
