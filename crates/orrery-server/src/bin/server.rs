use clap::Parser;
use orrery_server::{get_config, start_raft_node};
use std::path::PathBuf;

#[derive(clap::Parser)]
struct Args {
    #[arg(short = 'p', required = true)]
    path: PathBuf,
    #[arg(short = 'n', required = true)]
    num: u64,
    #[arg(short = 'j', required = true)]
    par: usize,
    #[arg(short = 'v')]
    verbose: bool,
}

fn main() {
    let args = Args::parse();

    if args.verbose {
        tracing_subscriber::fmt().with_level(true).init();
    }

    let cluster_config = get_config(args.path);
    let net_config = cluster_config
        .configs
        .into_iter()
        .find(|nc| nc.node_id == args.num)
        .unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(start_raft_node(
        net_config.node_id,
        net_config.addr,
        net_config.listen_at,
        args.par,
    ));
}
