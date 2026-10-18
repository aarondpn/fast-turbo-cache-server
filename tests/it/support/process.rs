//! Running the `fast-turbo-cache-server` binary: one-shot commands and long-running servers.
//! Every child runs with a cleared environment so the developer's environment cannot leak in.

use std::{
    io::{BufRead, BufReader, Read},
    process::{Child, Command, ExitStatus, Output, Stdio},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvTimeoutError},
    },
    time::{Duration, Instant},
};

const BIN: &str = env!("CARGO_BIN_EXE_fast-turbo-cache-server");

/// `PORT` for commands that must not reach a server: `healthcheck` against port 0 fails at once
/// (exit 1), and startup validation errors happen before binding. A "free" port obtained by
/// binding and dropping a listener could be taken by a concurrently started server.
pub const NO_SERVER_PORT: &str = "0";

/// The binary with a cleared environment, `RUST_LOG=info` and piped output.
pub fn command() -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env_clear()
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Runs the command and waits up to 15 s; kills it and panics if it does not exit.
pub fn run(mut cmd: Command) -> Output {
    let limit = Duration::from_secs(15);
    let mut child = cmd.spawn().expect("spawn fast-turbo-cache-server");
    let started = Instant::now();
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if started.elapsed() > limit {
            let _ = child.kill();
            let out = child.wait_with_output().unwrap();
            panic!(
                "process did not exit within {limit:?}\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Stdout followed by stderr.
pub fn combined(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// `fast-turbo-cache-server healthcheck` against `127.0.0.1:<port>`.
pub fn healthcheck(port: u16) -> Output {
    let mut cmd = command();
    cmd.env("HOST", "127.0.0.1")
        .env("PORT", port.to_string())
        .arg("healthcheck");
    run(cmd)
}

/// A running server process; killed on drop.
pub struct Server {
    child: Option<Child>,
    pub port: u16,
    /// Every stdout line so far, in order (the startup line included).
    stdout: Arc<Mutex<Vec<String>>>,
    /// Everything the server wrote to stderr so far.
    stderr: Arc<Mutex<String>>,
    /// The same stdout lines, for tests that follow the log.
    lines: Receiver<String>,
    /// Default `LOCAL_STORAGE_PATH` (removed after the process was killed).
    _data_dir: tempfile::TempDir,
}

impl Server {
    /// Starts the binary with `HOST=127.0.0.1 PORT=0 STORAGE_TYPE=local`, `LOCAL_STORAGE_PATH` in
    /// a fresh temporary directory, plus `envs` (which override these). `PORT=0` lets the OS pick
    /// a free port, so concurrent tests cannot race for one; the bound port is read from the
    /// startup log line. Returns once the `healthcheck` sub-command succeeds against it, so every
    /// server start also checks the healthcheck success path (exit 0).
    pub fn start(envs: &[(&str, &str)]) -> Self {
        let data_dir = tempfile::tempdir().unwrap();
        let mut cmd = command();
        cmd.env("HOST", "127.0.0.1")
            .env("PORT", "0")
            .env("STORAGE_TYPE", "local")
            .env("LOCAL_STORAGE_PATH", data_dir.path())
            .envs(envs.iter().copied());
        let mut child = cmd.spawn().expect("spawn fast-turbo-cache-server");

        let stdout = Arc::new(Mutex::new(Vec::new()));
        let (tx, lines) = mpsc::channel();
        let (pipe, sink) = (child.stdout.take().unwrap(), Arc::clone(&stdout));
        std::thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                sink.lock().unwrap().push(line.clone());
                let _ = tx.send(line);
            }
        });
        let stderr = Arc::new(Mutex::new(String::new()));
        let (mut pipe, sink) = (child.stderr.take().unwrap(), Arc::clone(&stderr));
        std::thread::spawn(move || {
            let mut chunk = [0u8; 4096];
            while let Ok(n @ 1..) = pipe.read(&mut chunk) {
                sink.lock()
                    .unwrap()
                    .push_str(&String::from_utf8_lossy(&chunk[..n]));
            }
        });

        let mut server = Server {
            child: Some(child),
            port: 0,
            stdout,
            stderr,
            lines,
            _data_dir: data_dir,
        };
        let started = Instant::now();
        loop {
            if server.port == 0 {
                let port = server
                    .stdout
                    .lock()
                    .unwrap()
                    .iter()
                    .find_map(|l| bound_port(l));
                server.port = port.unwrap_or(0);
            }
            if server.port != 0 && healthcheck(server.port).status.success() {
                return server;
            }
            if let Some(status) = server.child().try_wait().unwrap() {
                std::thread::sleep(Duration::from_millis(100)); // let the pipe readers drain
                panic!("server exited early with {status}:\n{}", server.output());
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "server did not become healthy:\n{}",
                server.output()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn child(&mut self) -> &mut Child {
        self.child.as_mut().unwrap()
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port)
    }

    /// The first stdout line.
    pub fn first_line(&self) -> String {
        self.stdout.lock().unwrap()[0].clone()
    }

    /// The next stdout line not returned yet (the startup lines come first).
    pub fn next_line(&self, timeout: Duration) -> Result<String, RecvTimeoutError> {
        self.lines.recv_timeout(timeout)
    }

    /// Everything the server wrote to stdout and stderr so far.
    pub fn output(&self) -> String {
        format!(
            "stdout:\n{}\nstderr:\n{}",
            self.stdout.lock().unwrap().join("\n"),
            self.stderr.lock().unwrap()
        )
    }

    /// Sends SIGTERM and waits up to `limit` for the exit; returns the elapsed time, the exit
    /// status and the server's output. Panics (after killing it) if it does not exit in time.
    #[cfg(unix)]
    pub fn terminate(&mut self, limit: Duration) -> (Duration, ExitStatus, String) {
        let pid = self.child().id().to_string();
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        let started = Instant::now();
        loop {
            if let Some(status) = self.child().try_wait().unwrap() {
                let elapsed = started.elapsed();
                self.child.take();
                std::thread::sleep(Duration::from_millis(100)); // let the pipe readers drain
                return (elapsed, status, self.output());
            }
            if started.elapsed() > limit {
                let mut child = self.child.take().unwrap();
                let _ = child.kill();
                let _ = child.wait();
                std::thread::sleep(Duration::from_millis(100));
                panic!(
                    "process did not exit within {limit:?} after SIGTERM:\n{}",
                    self.output()
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Port from the `Starting fast-turbo-cache-server` log line (text `address=127.0.0.1:N` or JSON
/// `"address":"127.0.0.1:N"`).
fn bound_port(line: &str) -> Option<u16> {
    if !line.contains("Starting fast-turbo-cache-server") {
        return None;
    }
    let rest = line.split("address").nth(1)?;
    let rest = rest.split("127.0.0.1:").nth(1)?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}
