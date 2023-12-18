use solve::invoker::Invoker;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let shutdown = CancellationToken::new();
    let invoker = Invoker::new();
    let invoker = tokio::spawn(invoker.run(shutdown.clone()));
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        shutdown.cancel();
    });
    invoker.await.unwrap().unwrap();
}
