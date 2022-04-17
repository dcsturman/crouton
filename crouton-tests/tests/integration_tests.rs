#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use log::{error, info, trace, warn};

    use crouton::CroutonClient;
    use std::sync::Once;
    use tokio::process::Command;
    use tokio::time::{sleep, Duration};

    static SERVER_PATH: &str = "../target/debug/crouton-server";
    static DEFAULT_COUNTER_NAME: &str = "VoteCounter";
    static DEFAULT_ACTOR_NAME: &str = "Me";
    static SECOND_ACTOR_NAME: &str = "Gul";

    static INIT: Once = Once::new();

    fn init_logger() {
        INIT.call_once(pretty_env_logger::init);
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
        let mut client = CroutonClient::new_and_wait(dst, DEFAULT_ACTOR_NAME).await;
        client.create(DEFAULT_COUNTER_NAME).await.unwrap();
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

        let mut client = CroutonClient::new_and_wait(dst, DEFAULT_ACTOR_NAME).await;

        client.create(DEFAULT_COUNTER_NAME).await.unwrap();
        let response = client.inc(DEFAULT_COUNTER_NAME).await.unwrap();
        assert_eq!(response, 1);
    }

    #[tokio::test]
    async fn pair_server() {
        init_logger();

        let mut _first = Command::new(SERVER_PATH)
            .args(["-p", "50055", "-s", "[::1]:50065"])
            .kill_on_drop(true)
            .spawn()
            .expect("First daemon failed to start.");

        let mut _second = Command::new(SERVER_PATH)
            .args(["-p", "50065", "-s", "[::1]:50055"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50055";
        let mut first_client = CroutonClient::new_and_wait(dst, DEFAULT_ACTOR_NAME).await;

        let dst = "http://[::1]:50065";
        let mut second_client = CroutonClient::new_and_wait(dst, SECOND_ACTOR_NAME).await;

        first_client.create(DEFAULT_COUNTER_NAME).await.unwrap();
        let _response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();

        info!("Sleep for 2 sec....");
        sleep(Duration::from_millis(2000)).await;

        let _response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();
        sleep(Duration::from_millis(2000)).await;

        let response = second_client
            .read(DEFAULT_COUNTER_NAME)
            .await
            .expect("Read error.");
        assert_eq!(response, 2);
    }

    #[tokio::test]
    async fn pair_server_comes_late() {
        init_logger();

        let mut _first = Command::new(SERVER_PATH)
            .args(["-p", "50057", "-s", "[::1]:50067"])
            .kill_on_drop(true)
            .spawn()
            .expect("First daemon failed to start.");

        let dst = "http://[::1]:50057";
        let mut first_client = CroutonClient::new_and_wait(dst, DEFAULT_ACTOR_NAME).await;

        let _response = first_client.create(DEFAULT_COUNTER_NAME).await.unwrap();
        let _response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();
        let _response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();

        let mut _second = Command::new(SERVER_PATH)
            .args(["-p", "50067", "-s", "[::1]:50057"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50065";

        let mut second_client = CroutonClient::new_and_wait(dst, SECOND_ACTOR_NAME).await;
        sleep(Duration::from_millis(4000)).await;

        let response = second_client
            .read(DEFAULT_COUNTER_NAME)
            .await
            .expect("Read error.");
        assert_eq!(response, 2);
    }

    #[tokio::test]
    async fn three_servers_one_late() {
        init_logger();

        let mut _first = Command::new(SERVER_PATH)
            .args(["-p", "50100", "-s", "[::1]:50110", "[::1]:50120"])
            .kill_on_drop(true)
            .spawn()
            .expect("First daemon failed to start.");

        let mut _second = Command::new(SERVER_PATH)
            .args(["-p", "50110", "-s", "[::1]:50100", "[::1]:50120"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50100";
        let mut first_client = CroutonClient::new_and_wait(dst, DEFAULT_ACTOR_NAME).await;
        first_client.create(DEFAULT_COUNTER_NAME).await.unwrap();
        let _response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();
        let _response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();

        let mut _third = Command::new(SERVER_PATH)
            .args(["-p", "50120", "-s", "[::1]:50100", "[::1]:50110"])
            .kill_on_drop(true)
            .spawn()
            .expect("Third (late) daemon failed to start.");

        let dst = "http://[::1]:50120";
        let mut second_client = CroutonClient::new_and_wait(dst, SECOND_ACTOR_NAME).await;

        sleep(Duration::from_millis(2000)).await;

        let response = second_client
            .read(DEFAULT_COUNTER_NAME)
            .await
            .expect("Read error.");
        assert_eq!(response, 2);
    }

    #[tokio::test]
    async fn survive_failure_reconnect() {
        init_logger();

        let mut _first = Command::new(SERVER_PATH)
            .args(["-p", "50200", "-s", "[::1]:50210", "[::1]:50220"])
            .kill_on_drop(true)
            .spawn()
            .expect("First daemon failed to start.");

        let mut second = Command::new(SERVER_PATH)
            .args(["-p", "50210", "-s", "[::1]:50200", "[::1]:50220"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50200";
        let mut first_client = CroutonClient::new_and_wait(dst, DEFAULT_ACTOR_NAME).await;

        first_client.create(DEFAULT_COUNTER_NAME).await.unwrap();
        let _response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();
        let _response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();

        info!("Sleep for 2 sec....");
        sleep(Duration::from_millis(2000)).await;

        second.kill().await.unwrap();

        let _response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();
        let response = first_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();

        assert_eq!(response, 4);

        info!("Sleep for 2 sec....");
        sleep(Duration::from_millis(2000)).await;

        let mut _second = Command::new(SERVER_PATH)
            .args(["-p", "50210", "-s", "[::1]:50200", "[::1]:50220"])
            .kill_on_drop(true)
            .spawn()
            .expect("Second daemon failed to start.");

        let dst = "http://[::1]:50210";
        let mut second_client = CroutonClient::new_and_wait(dst, SECOND_ACTOR_NAME).await;
        let _response = second_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();
        let response = second_client.inc(DEFAULT_COUNTER_NAME).await.unwrap();
        assert_eq!(response, 6);
    }
}
