#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use log::{error, info, trace, warn};

    use crouton_protos::catalog::catalog_client::CatalogClient;
    use crouton_protos::catalog::QueryRequest;
    use std::sync::Once;
    use tokio::process::Command;
    use tokio::time::{sleep, Duration};
    use tonic::transport::Channel;

    static SERVER_PATH: &str = "../target/debug/crouton-server";
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

        let mut _daemon = Command::new(SERVER_PATH)
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

        let mut _daemon = Command::new(SERVER_PATH)
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

        let mut _first = Command::new(SERVER_PATH)
            .args(["-p", "50055", "-s", "[::1]:50066"])
            .kill_on_drop(true)
            .spawn()
            .expect("First daemon failed to start.");

        let mut _second = Command::new(SERVER_PATH)
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

        info!("Sleep for 2 sec....");
        sleep(Duration::from_millis(2000)).await;
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

    #[tokio::test]
    async fn pair_server_comes_late() {
        init_logger();

        let mut _first = Command::new(SERVER_PATH)
            .args(["-p", "50057", "-s", "[::1]:50068"])
            .kill_on_drop(true)
            .spawn()
            .expect("First daemon failed to start.");

        let dst = "http://[::1]:50057";
        let mut first_client = wait_on_connect(dst.to_string()).await;

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

        info!("Sleep for 2 sec....");
        sleep(Duration::from_millis(2000)).await;
        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let _response = first_client.inc(request).await.unwrap();

        let mut _second = Command::new(SERVER_PATH)
            .args(["-p", "50067", "-s", "[::1]:50058"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50065";
        let mut second_client = wait_on_connect(dst.to_string()).await;

        sleep(Duration::from_millis(2000)).await;

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Two".to_string(),
        });

        let response = second_client.read(request).await.expect("Read error.");
        assert_eq!(response.get_ref().value, 2);
    }

    #[tokio::test]
    async fn three_servers_one_late() {
        init_logger();

        let mut _first = Command::new(SERVER_PATH)
            .args(["-p", "50100", "-s", "[::1]:50111", "[::1]:50121"])
            .kill_on_drop(true)
            .spawn()
            .expect("First daemon failed to start.");

        let mut _second = Command::new(SERVER_PATH)
            .args(["-p", "50110", "-s", "[::1]:50101", "[::1]:50121"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50100";
        let mut first_client = wait_on_connect(dst.to_string()).await;

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

        info!("Sleep for 2 sec....");
        sleep(Duration::from_millis(2000)).await;
        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let _response = first_client.inc(request).await.unwrap();

        let mut _third = Command::new(SERVER_PATH)
            .args(["-p", "50120", "-s", "[::1]:50101", "[::1]:50111"])
            .kill_on_drop(true)
            .spawn()
            .expect("Third (late) daemon failed to start.");

        let dst = "http://[::1]:50120";
        let mut second_client = wait_on_connect(dst.to_string()).await;

        sleep(Duration::from_millis(2000)).await;

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "Two".to_string(),
        });

        let response = second_client.read(request).await.expect("Read error.");
        assert_eq!(response.get_ref().value, 2);
    }

    #[tokio::test]
    async fn survive_failure_reconnect() {
        init_logger();

        let mut _first = Command::new(SERVER_PATH)
            .args(["-p", "50200", "-s", "[::1]:50211", "[::1]:50221"])
            .kill_on_drop(true)
            .spawn()
            .expect("First daemon failed to start.");

        let mut second = Command::new(SERVER_PATH)
            .args(["-p", "50210", "-s", "[::1]:50201", "[::1]:50221"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50200";
        let mut first_client = wait_on_connect(dst.to_string()).await;

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

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let _response = first_client.inc(request).await.unwrap();

        info!("Sleep for 2 sec....");
        sleep(Duration::from_millis(2000)).await;

        second.kill().await.unwrap();

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let _response = first_client.inc(request).await.unwrap();

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let response = first_client.inc(request).await.unwrap();

        assert_eq!(response.get_ref().value, 4);

        info!("Sleep for 2 sec....");
        sleep(Duration::from_millis(2000)).await;

        let mut _second = Command::new(SERVER_PATH)
            .args(["-p", "50210", "-s", "[::1]:50201", "[::1]:50221"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50210";
        let mut second_client = wait_on_connect(dst.to_string()).await;
        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let _response = second_client.inc(request).await.unwrap();

        let request = tonic::Request::new(QueryRequest {
            name: "VoteCounter".into(),
            actor: "One".to_string(),
        });
        let response = second_client.inc(request).await.unwrap();
        assert_eq!(response.get_ref().value, 6);
    }
}
