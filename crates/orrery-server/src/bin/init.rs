use clap::Parser;
use maplit::btreeset;
use orrery_server::client::Client;
use orrery_server::get_config;
use orrery_server::raft::NodeId;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Parser)]
struct Args {
    #[arg(short = 'p', required = true)]
    path: PathBuf,
    nodes: Vec<NodeId>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let net_configs: Vec<(NodeId, String)> = get_config(args.path)
        .configs
        .into_iter()
        .map(|cfg| (cfg.node_id, cfg.addr))
        .collect();

    let numbers = args.nodes;
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let client = Arc::new(Client::new(0, net_configs[0].1.to_string()));

    println!("Initializing cluster with (1) node");
    client.init().await.unwrap();

    println!("Metrics: {:?}", client.metrics().await.unwrap());

    if !numbers.is_empty() {
        println!("Adding learners...");
        for &number in &numbers {
            if number != 0 {
                let i = net_configs.iter().position(|nc| nc.0 == number).unwrap();
                client
                    .add_learner((net_configs[i].0, net_configs[i].1.to_string()))
                    .await
                    .unwrap();
            }
        }
        println!("Adding learners... done");
        client
            .change_membership(&BTreeSet::from_iter(numbers.into_iter()))
            .await
            .unwrap();
    }
}
