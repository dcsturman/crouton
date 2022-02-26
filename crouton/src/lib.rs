#[allow(unused_imports)]
use log::{error, info, trace, warn};

use crouton_protos::catalog::catalog_client::CatalogClient;
use crouton_protos::catalog::QueryRequest;
use tokio::time::{sleep, Duration};
use tonic::transport::Channel;
use tonic::Status;

pub struct CroutonClient {
    actor_name: String,
    client: CatalogClient<Channel>,
}

impl CroutonClient {
    pub async fn new(
        dst: &str,
        actor_name: &str,
    ) -> Result<CroutonClient, tonic::transport::Error> {
        let actor_name = actor_name.to_string();
        let client = CatalogClient::connect(dst.to_string()).await?;

        Ok(CroutonClient { actor_name, client })
    }

    pub async fn new_and_wait(dst: &str, actor_name: &str) -> CroutonClient {
        let actor_name = actor_name.to_string();
        loop {
            let connection = CatalogClient::connect(dst.to_string()).await.ok();
            if let Some(client) = connection {
                return CroutonClient { actor_name, client };
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn create(&mut self, data_name: &str) -> Result<(), Status> {
        let request = tonic::Request::new(QueryRequest {
            name: data_name.to_string(),
            actor: self.actor_name.clone(),
        });

        let _response = self.client.create(request).await?;
        Ok(())
    }

    pub async fn inc(&mut self, data_name: &str) -> Result<i32, Status> {
        let request = tonic::Request::new(QueryRequest {
            name: data_name.to_string(),
            actor: self.actor_name.clone(),
        });

        let response = self.client.inc(request).await?;
        Ok(response.get_ref().value)
    }

    pub async fn read(&mut self, data_name: &str) -> Result<i32, Status> {
        let request = tonic::Request::new(QueryRequest {
            name: data_name.to_string(),
            actor: self.actor_name.clone(),
        });

        let response = self.client.read(request).await?;
        Ok(response.get_ref().value)
    }
}
