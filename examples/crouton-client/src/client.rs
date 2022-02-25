use clap::{App, Arg};
use crouton_protos::catalog::catalog_client::CatalogClient;
use crouton_protos::catalog::QueryRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("Crouton (test) Client")
        .version("0.1")
        .author("Daniel Sturman <dan@sturman.org>")
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("Port for target server.")
                .takes_value(true),
        )
        .get_matches();

    let port = matches.value_of("port").unwrap_or("50051");
    let dst = format!("http://[::1]:{}", port);

    let mut client = CatalogClient::connect(dst).await?;

    let request = tonic::Request::new(QueryRequest {
        name: "VoteCounter".into(),
        actor: "Me".to_string(),
    });

    let response = client.create(request).await?;

    println!("RESPONSE={:?}", response);

    let request = tonic::Request::new(QueryRequest {
        name: "VoteCounter".into(),
        actor: "Me".to_string(),
    });

    client.inc(request).await?;

    let request = tonic::Request::new(QueryRequest {
        name: "VoteCounter".into(),
        actor: "Me".to_string(),
    });

    let response = client.inc(request).await?;
    println!("RESPONSE={:?}", response);

    let request = tonic::Request::new(QueryRequest {
        name: "VoteCounter".into(),
        actor: "Me".to_string(),
    });

    let response = client.read(request).await?;
    println!("RESPONSE={:?}", response);

    Ok(())
}
