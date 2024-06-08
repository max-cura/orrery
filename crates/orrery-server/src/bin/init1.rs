use maplit::btreeset;
use orrery_server::client::Client;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let net_configs = vec![
        (0, "34.94.189.191:80"),
        // (1, "127.0.0.1:23002", "0.0.0.0:23002"),
        // (2, "127.0.0.1:23003", "0.0.0.0:23003"),
    ];

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let client = Arc::new(Client::new(0, net_configs[0].1.to_string()));

    println!("Initializing cluster with (1) node");
    client.init().await.unwrap();

    println!("Metrics: {:?}", client.metrics().await.unwrap());
    // println!("Adding learners...");
    // client
    //     .add_learner((net_configs[1].0, net_configs[1].1.to_string()))
    //     .await
    //     .unwrap();
    // client
    //     .add_learner((net_configs[2].0, net_configs[2].1.to_string()))
    //     .await
    //     .unwrap();
    //
    // println!("Adding learners... done");
    // println!("Metrics: {:?}", client.metrics().await.unwrap());
    //
    // client.change_membership(&btreeset! {0,1,2}).await.unwrap();
}
