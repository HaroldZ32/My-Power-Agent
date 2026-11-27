mod mcp;
mod store;

use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt};
use std::os::unix::net::UnixStream as BlockingStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use store::{Error, Store, Wake};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch, Semaphore};

const FRAME: usize = 64 * 1024;
const MAX_CLIENTS: usize = 2048;

struct Job {
    agent: String,
    request: Arc<Value>,
    response: oneshot::Sender<Value>,
}

type Events = BTreeMap<String, [watch::Sender<()>; 3]>;
type Roster = BTreeMap<String, String>;
type MainResult<T> = Result<T, Box<dyn std::error::Error>>;

fn failure(code: &str) -> Value {
    json!({"ok":false, "error":code})
}

fn success(mut value: Value) -> Value {
    value["ok"] = true.into();
    value
}

fn worker(mut store: Store, mut jobs: mpsc::Receiver<Job>, events: &Events) {
    while let Some(job) = jobs.blocking_recv() {
        let result = match store.execute(&job.agent, &job.request) {
            Ok((result, notification)) => {
                if let Some((kind, targets)) = notification {
                    for target in targets {
                        if let Some(event) = events.get(&target) {
                            event[kind as usize].send_replace(());
                        }
                    }
                }
                success(result)
            }
            Err(Error::Request(code)) => failure(&code),
            Err(Error::Storage(error)) => {
                eprintln!("mailbox storage error: {error}");
                failure("storage_error")
            }
        };
        let _ = job.response.send(result);
    }
}

struct Channel {
    roster: Roster,
    events: Arc<Events>,
    jobs: mpsc::Sender<Job>,
}

impl Channel {
    async fn request(&self, request: Value) -> Value {
        let Some(agent) = request["agent_id"].as_str() else {
            return failure("unauthorized");
        };
        let Some(token) = request["token"].as_str() else {
            return failure("unauthorized");
        };
        if !self
            .roster
            .get(agent)
            .is_some_and(|key| bool::from(key.as_bytes().ct_eq(token.as_bytes())))
        {
            return failure("unauthorized");
        }
        if request["op"] == "whoami" {
            return success(json!({"agent_id":agent}));
        }
        let can_wait = matches!(request["op"].as_str(), Some("receive" | "meeting_status"));
        let wait = if can_wait {
            match request.get("wait_ms").map_or(Some(0), Value::as_u64) {
                Some(n @ 0..=60_000) => n,
                _ => return failure("invalid_wait_ms"),
            }
        } else {
            0
        };
        let until = if request["op"] == "meeting_status" {
            match request.get("until").map(Value::as_str) {
                Some(Some(phase @ ("exchange" | "complete"))) => Some(phase),
                Some(_) => return failure("invalid_until"),
                None => None,
            }
        } else {
            None
        };
        let event = match until {
            Some("exchange") => Wake::Exchange,
            Some("complete") => Wake::Complete,
            _ => Wake::Mail,
        };
        let until = until.map(str::to_owned);
        let agent = agent.to_owned();
        let mut changes = self.events[&agent][event as usize].subscribe();
        let request = Arc::new(request);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait);
        loop {
            // Mark the observed generation before reading so a concurrent commit cannot be missed.
            changes.borrow_and_update();
            let (response, result) = oneshot::channel();
            if self
                .jobs
                .send(Job {
                    agent: agent.clone(),
                    request: request.clone(),
                    response,
                })
                .await
                .is_err()
            {
                return failure("storage_error");
            }
            let Ok(value) = result.await else {
                return failure("storage_error");
            };
            let pending = match request["op"].as_str() {
                Some("receive") => value["messages"].as_array().is_some_and(Vec::is_empty),
                Some("meeting_status") if until.as_deref() == Some("exchange") => {
                    value["phase"] == "preparing"
                }
                Some("meeting_status") if until.as_deref() == Some("complete") => {
                    value["ok"] == true && value["phase"] != "complete"
                }
                _ => false,
            };
            if !pending || tokio::time::Instant::now() >= deadline {
                return value;
            }
            if tokio::time::timeout_at(deadline, changes.changed())
                .await
                .is_err()
            {
                return value;
            }
        }
    }

    async fn handle(&self, stream: UnixStream) -> io::Result<()> {
        let (read, mut write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let mut frame = Vec::new();
        loop {
            frame.clear();
            let mut limited = (&mut reader).take((FRAME + 1) as u64);
            let size = match tokio::time::timeout(
                Duration::from_secs(75),
                limited.read_until(b'\n', &mut frame),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => return Ok(()),
            };
            if size == 0 {
                return Ok(());
            }
            let response = if size > FRAME {
                failure("frame_too_large")
            } else if !frame.ends_with(b"\n") {
                return Ok(());
            } else {
                match serde_json::from_slice(&frame) {
                    Ok(request) => self.request(request).await,
                    Err(_) => failure("invalid_json"),
                }
            };
            let mut bytes = response.to_string().into_bytes();
            bytes.push(b'\n');
            match tokio::time::timeout(Duration::from_secs(5), write.write_all(&bytes)).await {
                Ok(result) => result?,
                Err(_) => return Ok(()),
            }
            if size > FRAME {
                return Ok(());
            }
        }
    }
}

fn parse_roster(path: &Path) -> MainResult<Roster> {
    let roster: Roster = serde_json::from_slice(&fs::read(path)?)?;
    let tokens: BTreeSet<_> = roster.values().collect();
    if !(1..=1024).contains(&roster.len())
        || roster.len() != tokens.len()
        || roster.iter().any(|(id, token)| {
            !(1..=18).contains(&id.len())
                || id.starts_with('0')
                || !id.bytes().all(|b| b.is_ascii_digit())
                || !(32..=128).contains(&token.len())
                || !token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        })
    {
        return Err("roster must map 1–1024 distinct numeric agent IDs to unique tokens".into());
    }
    Ok(roster)
}

async fn serve(directory: &Path, roster_path: &Path, capacity: i64) -> MainResult<()> {
    let roster = parse_roster(roster_path)?;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(directory)?;
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.is_dir()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(
            "state directory must be owned by the current user and private (chmod 700)".into(),
        );
    }

    let lock = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("channel.lock"))?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(io::Error::last_os_error().into());
    }

    let path = directory.join("channel.sock");
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_socket() => fs::remove_file(&path)?,
        Ok(_) => return Err("channel.sock must be a Unix socket".into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let store = Store::open(
        &directory.join("mailbox.sqlite3"),
        capacity,
        roster.keys().cloned().collect(),
    )?;
    use tokio::signal::unix::{signal, SignalKind};
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let listener = UnixListener::bind(&path)?;
    let events = Arc::new(
        roster
            .keys()
            .cloned()
            .map(|id| (id, std::array::from_fn(|_| watch::channel(()).0)))
            .collect(),
    );
    let (jobs, receiver) = mpsc::channel(MAX_CLIENTS);
    let channel = Arc::new(Channel {
        roster,
        events,
        jobs,
    });
    let worker_events = channel.events.clone();
    let worker_thread = std::thread::spawn(move || worker(store, receiver, &worker_events));

    let limit = Arc::new(Semaphore::new(MAX_CLIENTS));
    let mut clients = tokio::task::JoinSet::new();
    eprintln!("Listening on {}", path.display());
    let result = loop {
        tokio::select! {
            _ = interrupt.recv() => break Ok(()),
            _ = terminate.recv() => break Ok(()),
            _ = clients.join_next(), if !clients.is_empty() => {}
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    if let Ok(permit) = limit.clone().try_acquire_owned() {
                        let channel = channel.clone();
                        clients.spawn(async move {
                            let _permit = permit;
                            let _ = channel.handle(stream).await;
                        });
                    } else {
                        let _ = stream.try_write(b"{\"ok\":false,\"error\":\"busy\"}\n");
                    }
                }
                Err(error) => break Err(error),
            }
        }
    };
    drop(listener);
    clients.shutdown().await;
    drop(channel);
    worker_thread
        .join()
        .map_err(|_| "storage worker panicked")?;
    fs::remove_file(path)?;
    result?;
    Ok(())
}

fn call(directory: &Path, arg: &str) -> MainResult<bool> {
    let mut input = Vec::new();
    if arg == "-" {
        use std::io::Read;
        io::stdin()
            .take((FRAME + 1) as u64)
            .read_to_end(&mut input)?;
    } else {
        input.extend_from_slice(arg.as_bytes());
    }
    if input.len() > FRAME {
        return Err("request exceeds frame limit".into());
    }
    let mut request: Value = serde_json::from_slice(&input)?;
    if !request.is_object() {
        return Err("request must be a JSON object".into());
    }
    request["agent_id"] = std::env::var("AGENT_CHANNEL_ID")?.into();
    request["token"] = std::env::var("AGENT_CHANNEL_TOKEN")?.into();
    let mut bytes = request.to_string().into_bytes();
    bytes.push(b'\n');
    if bytes.len() > FRAME {
        return Err("request exceeds frame limit".into());
    }
    let mut socket = BlockingStream::connect(directory.join("channel.sock"))?;
    socket.set_read_timeout(Some(Duration::from_secs(75)))?;
    socket.set_write_timeout(Some(Duration::from_secs(5)))?;
    socket.write_all(&bytes)?;
    let mut response = String::new();
    io::BufReader::new(socket).read_line(&mut response)?;
    let value: Value = serde_json::from_str(&response)?;
    print!("{response}");
    Ok(value["ok"] == true)
}

async fn run() -> MainResult<bool> {
    let mut args = std::env::args().skip(1).peekable();
    let mut directory = std::env::var_os("AGENT_CHANNEL_STATE_DIR")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".agent-channel"));
    if args.peek().is_some_and(|arg| arg == "--state-dir") {
        args.next();
        directory = args.next().ok_or("missing --state-dir path")?.into();
    }
    match args.next().as_deref() {
        Some("serve") => {
            let mut roster = None;
            let mut capacity = 256;
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--roster" => roster = args.next().map(PathBuf::from),
                    "--capacity" => capacity = args.next().ok_or("missing capacity")?.parse()?,
                    _ => return Err(format!("unknown option: {flag}").into()),
                }
            }
            if !(1..=100_000).contains(&capacity) {
                return Err("--capacity must be between 1 and 100000".into());
            }
            serve(
                &directory,
                &roster.ok_or("missing --roster file")?,
                capacity,
            )
            .await?;
            Ok(true)
        }
        Some("mcp") if args.next().is_none() => {
            mcp::serve(&directory).await?;
            Ok(true)
        }
        Some("call") => {
            let request = args.next().ok_or("missing JSON request")?;
            if args.next().is_some() {
                return Err("unexpected argument after JSON request".into());
            }
            call(&directory, &request)
        }
        Some("--help" | "-h") | None => {
            println!("agent-channel [--state-dir DIR] serve --roster FILE [--capacity N]\nagent-channel [--state-dir DIR] call JSON|-\nagent-channel [--state-dir DIR] mcp");
            Ok(true)
        }
        Some(command) => Err(format!("unknown command: {command}").into()),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(true) => std::process::ExitCode::SUCCESS,
        Ok(false) => std::process::ExitCode::FAILURE,
        Err(error) => {
            eprintln!("agent-channel: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
