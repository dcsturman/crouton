#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use log::{error, info, trace, warn};

    use crouton::catalog::catalog_client::CatalogClient;
    use crouton::catalog::QueryRequest;
    use std::sync::Once;
    use tokio::process::Command;
    use tokio::time::{sleep, Duration};
    use tonic::transport::Channel;

    static INIT: Once = Once::new();

    fn init_logger() {
        INIT.call_once(pretty_env_logger::init);
    }

    async fn wait_on_connect(dst: String) -> CatalogClient<Channel> {
        loop {
            let connection = CatalogClient::connect(dst.clone()).await.ok();
            if let Some(valid_connection) = connection {
                return valid_connection;
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    async fn create_counter() {
        init_logger();

        let mut _daemon = Command::new("./target/debug/crouton-server")
            .kill_on_drop(true)
            .spawn()
            .expect("Daemon failed to start.");

        sleep(Duration::from_millis(1000)).await;

        let dst = "http://[::1]:50051";
        let mut client = wait_on_connect(dst.to_string()).await;

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = client.create(request).await.unwrap();
        assert_eq!(response.get_ref().status, 0);
    }

    #[tokio::test]
    async fn inc_counter() {
        init_logger();

        let mut _daemon = Command::new("./target/debug/crouton-server")
            .args(["-p", "50053"])
            .kill_on_drop(true)
            .spawn()
            .expect("Daemon failed to start.");

        let dst = "http://[::1]:50053";

        let mut client = wait_on_connect(dst.to_string()).await;

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = client.create(request).await.unwrap();
        assert_eq!(response.get_ref().status, 0);

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Me".to_string(),
        });

        let response = client.inc(request).await.unwrap();
        assert_eq!(response.get_ref().value, 1);
    }

    #[tokio::test]
    async fn pair_server() {
        init_logger();

        let mut _first = Command::new("./target/debug/crouton-server")
            .args(["-p", "50055", "-s", "[::1]:50066"])
            .kill_on_drop(true)
            .spawn()
            .expect("First daemon failed to start.");

        let mut _second = Command::new("./target/debug/crouton-server")
            .args(["-p", "50065", "-s", "[::1]:50056"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50055";
        let mut first_client = wait_on_connect(dst.to_string()).await;

        let dst = "http://[::1]:50065";
        let mut second_client = wait_on_connect(dst.to_string()).await;

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let response = first_client.create(request).await.unwrap();
        assert_eq!(response.get_ref().status, 0);

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let _response = first_client.inc(request).await.unwrap();

        info!("Sleep for 12 sec....");
        sleep(Duration::from_millis(12000)).await;
        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let _response = first_client.inc(request).await.unwrap();
        sleep(Duration::from_millis(2000)).await;

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Two".to_string(),
        });

        let response = second_client.read(request).await.expect("Read error.");
        assert_eq!(response.get_ref().value, 2);
    }
}
