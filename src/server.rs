//! HTTP server (`axum::serve`) with a bounded graceful shutdown, plus helpers for signal
//! handling and the `healthcheck` sub-command.
//!
//! Connection limits, request header timeouts and slowloris protection are left to the
//! TLS-terminating reverse proxy in front of the server (docs/deployment.md).

use std::{future::Future, io, net::SocketAddr, time::Duration};

use axum::{Router, serve::ListenerExt};
use futures_util::FutureExt;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::config::ServerOptions;

/// Timeout for the `healthcheck` sub-command probe.
pub const HEALTHCHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// Serves `app` (HTTP/1 and cleartext HTTP/2) on `listener` until `shutdown` resolves.
///
/// On shutdown the listener is closed and open connections are asked to finish their in-flight
/// requests (idle keep-alive connections are closed right away). After
/// `options.shutdown_grace` the function returns regardless of stuck connections; their tasks
/// are abandoned with the runtime (see `main`).
pub async fn serve<F>(
    listener: TcpListener,
    app: Router,
    options: &ServerOptions,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let shutdown = shutdown.shared();
    let listener = listener.tap_io(|stream| {
        let _ = stream.set_nodelay(true);
    });
    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown.clone());
    let grace = options.shutdown_grace;
    let deadline = async move {
        shutdown.await;
        info!(
            grace_secs = grace.as_secs_f64(),
            "Stopped accepting connections; waiting for in-flight requests"
        );
        tokio::time::sleep(grace).await;
    };
    tokio::select! {
        result = server => {
            result?;
            info!("All connections closed");
        }
        () = deadline => warn!("Shutdown grace period elapsed; closing remaining connections"),
    }
    Ok(())
}

/// Awaits a signal future. If the signal handler could not be registered the error is logged and
/// the returned future never resolves, so a registration failure can never trigger an immediate
/// shutdown.
pub async fn wait_for_signal<F>(name: &'static str, signal: F)
where
    F: Future<Output = io::Result<()>>,
{
    match signal.await {
        Ok(()) => info!(signal = name, "Received shutdown signal"),
        Err(err) => {
            error!(signal = name, error = %err, "Failed to listen for signal; it will be ignored");
            std::future::pending::<()>().await;
        }
    }
}

/// Resolves on SIGINT (Ctrl-C) or SIGTERM (unix).
pub async fn shutdown_signal() {
    let ctrl_c = wait_for_signal("SIGINT", tokio::signal::ctrl_c());

    #[cfg(unix)]
    let terminate = wait_for_signal("SIGTERM", async {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        match sigterm.recv().await {
            Some(()) => Ok(()),
            None => Err(io::Error::other("SIGTERM stream closed")),
        }
    });

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

/// Performs `GET http://<addr>/healthz`; returns `Ok(())` on HTTP 200.
pub async fn run_healthcheck(addr: SocketAddr) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(HEALTHCHECK_TIMEOUT)
        .no_proxy()
        .build()?;
    let url = format!("http://{addr}/healthz");
    let response = client.get(&url).send().await?;
    if response.status() == reqwest::StatusCode::OK {
        Ok(())
    } else {
        anyhow::bail!("{url} returned HTTP {}", response.status())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use axum::{Router, routing::get};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::oneshot,
    };

    use super::{run_healthcheck, serve, wait_for_signal};
    use crate::config::ServerOptions;

    fn test_app() -> Router {
        Router::new()
            .route("/healthz", get(|| async { "ok" }))
            .route(
                "/slow-handler",
                get(|| async {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    "slow-done"
                }),
            )
            .route(
                "/hang",
                get(|| async {
                    std::future::pending::<()>().await;
                    "never"
                }),
            )
    }

    struct Running {
        addr: std::net::SocketAddr,
        stop: Option<oneshot::Sender<()>>,
        done: tokio::task::JoinHandle<std::io::Result<()>>,
    }

    async fn start(shutdown_grace: Duration) -> Running {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let options = ServerOptions {
            shutdown_grace,
            ..ServerOptions::default()
        };
        let done = tokio::spawn(async move {
            serve(listener, test_app(), &options, async move {
                let _ = rx.await;
            })
            .await
        });
        Running {
            addr,
            stop: Some(tx),
            done,
        }
    }

    /// Reads until EOF (or error) and returns everything read; panics after `limit`.
    async fn read_to_close(stream: &mut TcpStream, limit: Duration) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = [0u8; 1024];
        let result = tokio::time::timeout(limit, async {
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => out.extend_from_slice(&buf[..n]),
                }
            }
        })
        .await;
        assert!(result.is_ok(), "connection was not closed within {limit:?}");
        out
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_forever_for_stuck_requests() {
        let mut server = start(Duration::from_millis(500)).await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream
            .write_all(b"GET /hang HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let started = Instant::now();
        server.stop.take().unwrap().send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), &mut server.done)
            .await
            .expect("serve() did not return after the shutdown grace period");
        result.unwrap().unwrap();
        assert!(started.elapsed() >= Duration::from_millis(400));
    }

    #[tokio::test]
    async fn graceful_shutdown_lets_in_flight_requests_finish() {
        let mut server = start(Duration::from_secs(10)).await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream
            .write_all(b"GET /slow-handler HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let started = Instant::now();
        server.stop.take().unwrap().send(()).unwrap();
        let data = read_to_close(&mut stream, Duration::from_secs(5)).await;
        let text = String::from_utf8_lossy(&data);
        assert!(text.starts_with("HTTP/1.1 200"), "unexpected: {text}");
        assert!(text.ends_with("slow-done"), "unexpected: {text}");
        // serve() returns once the last connection is closed, long before the grace period.
        tokio::time::timeout(Duration::from_secs(2), &mut server.done)
            .await
            .expect("serve() did not return after the last request finished")
            .unwrap()
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn graceful_shutdown_returns_quickly_when_idle() {
        let mut server = start(Duration::from_secs(30)).await;
        // An idle keep-alive connection does not delay the shutdown.
        let mut idle = TcpStream::connect(server.addr).await.unwrap();
        idle.write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 256];
        let n = idle.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));

        server.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut server.done)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(TcpStream::connect(server.addr).await.is_err());
        let rest = read_to_close(&mut idle, Duration::from_secs(2)).await;
        assert!(rest.is_empty(), "unexpected data: {rest:?}");
    }

    #[tokio::test]
    async fn healthcheck_reports_up_and_down() {
        let mut server = start(Duration::from_secs(30)).await;
        run_healthcheck(server.addr).await.unwrap();

        server.stop.take().unwrap().send(()).unwrap();
        (&mut server.done).await.unwrap().unwrap();
        assert!(run_healthcheck(server.addr).await.is_err());
    }

    #[tokio::test]
    async fn failed_signal_registration_never_triggers_shutdown() {
        let fut = wait_for_signal("TEST", async {
            Err(std::io::Error::other("registration failed"))
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(200), fut)
                .await
                .is_err(),
            "a failed signal registration must not resolve the shutdown future"
        );
        let ok = wait_for_signal("TEST", async { Ok(()) });
        tokio::time::timeout(Duration::from_millis(200), ok)
            .await
            .expect("a delivered signal must resolve");
    }
}
