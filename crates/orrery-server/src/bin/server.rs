use clap::Parser;
use orrery_server::start_raft_node;

#[derive(clap::Parser)]
struct Node {
    #[arg(short = 'n', required = true)]
    num: usize,
    #[arg(short = 'j', required = true)]
    par: usize,
}

fn main() {
    // tracing_subscriber::fmt().with_level(true).init();

    let node_config = Node::parse();

    let net_configs = vec![
        (0, "127.0.0.1:80", "0.0.0.0:80"),
        // (1, "127.0.0.1:23002", "0.0.0.0:23002"),
        // (2, "127.0.0.1:23003", "0.0.0.0:23003"),
    ];

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(start_raft_node(
        net_configs[node_config.num].0,
        net_configs[node_config.num].1.to_string(),
        net_configs[node_config.num].2.to_string(),
        node_config.par,
    ));
}
