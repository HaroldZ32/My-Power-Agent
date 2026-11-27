use serde_json::{json, Value};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BIN: &str = env!("CARGO_BIN_EXE_agent-channel");

fn token(id: &str) -> String {
    id.repeat(32)
}

struct Fixture {
    root: PathBuf,
    process: Option<Child>,
}

impl Fixture {
    fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("ach-{stamp}"));
        fs::create_dir(&root).unwrap();
        fs::write(
            root.join("roster"),
            json!({
                "1": token("1"), "2": token("2"), "3": token("3"), "4": token("4")
            })
            .to_string(),
        )
        .unwrap();
        let mut fixture = Self {
            root,
            process: None,
        };
        fixture.start();
        fixture
    }

    fn start(&mut self) {
        let mut child = Command::new(BIN)
            .arg("--state-dir")
            .arg(self.root.join("s"))
            .arg("serve")
            .arg("--roster")
            .arg(self.root.join("roster"))
            .args(["--capacity", "128"])
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stderr.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        self.process = Some(child);
        assert!(line.starts_with("Listening"), "{line}");
    }

    fn crash(&mut self) {
        if let Some(mut child) = self.process.take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }

    fn socket(&self) -> PathBuf {
        self.root.join("s/channel.sock")
    }

    fn call(&self, id: &str, request: Value) -> Value {
        call(&self.socket(), id, request)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.crash();
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn call(path: &Path, id: &str, mut request: Value) -> Value {
    let mut socket = UnixStream::connect(path).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(4)))
        .unwrap();
    request["agent_id"] = id.into();
    if request.get("token").is_none() {
        request["token"] = token(id).into();
    }
    writeln!(socket, "{request}").unwrap();
    let mut line = String::new();
    BufReader::new(socket).read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap()
}

fn waiting(fixture: &Fixture, id: &'static str, request: Value) -> mpsc::Receiver<Value> {
    let path = fixture.socket();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        tx.send(call(&path, id, request)).unwrap();
    });
    rx
}

#[test]
fn queue_routing_atomicity_and_recovery() {
    let mut fixture = Fixture::new();
    let cli = Command::new(BIN)
        .arg("--state-dir")
        .arg(fixture.root.join("s"))
        .args(["call", r#"{"op":"whoami"}"#])
        .env("AGENT_CHANNEL_ID", "1")
        .env("AGENT_CHANNEL_TOKEN", token("1"))
        .output()
        .unwrap();
    assert!(cli.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&cli.stdout).unwrap()["agent_id"],
        "1"
    );
    assert_eq!(
        fixture.call("1", json!({"op":"whoami","token":token("2")}))["error"],
        "unauthorized"
    );

    let receive = waiting(&fixture, "2", json!({"op":"receive","wait_ms":2000}));
    std::thread::sleep(Duration::from_millis(25));
    let send = json!({"op":"send","to":"2","request_id":"task","payload":{"task":"work"}});
    let now = Instant::now();
    let sent = fixture.call("1", send.clone());
    let message = receive.recv_timeout(Duration::from_secs(1)).unwrap()["messages"][0].clone();
    eprintln!("Durable send and receiver wake: {:?}", now.elapsed());
    let id = message["id"].clone();
    assert_eq!(id, sent["messages"][0]["id"]);
    assert_eq!(fixture.call("2", json!({"op":"inbox"}))["pending"], 1);
    assert_eq!(
        fixture.call("2", json!({"op":"receive","after_id":id}))["messages"],
        json!([])
    );
    assert_eq!(
        fixture.call("3", json!({"op":"ack","ids":[id]}))["error"],
        "unknown_message"
    );
    assert_eq!(
        fixture.call("2", json!({"op":"ack","ids":[id,999999]}))["error"],
        "unknown_message"
    );
    assert_eq!(fixture.call("2", json!({"op":"inbox"}))["pending"], 1);
    assert_eq!(
        fixture.call("2", json!({"op":"ack","ids":[id]}))["ok"],
        true
    );
    let reply = fixture.call(
        "2",
        json!({"op":"send","to":"1","request_id":"reply","payload":"ok","reply_to":id}),
    );
    assert_eq!(reply["thread_id"], sent["thread_id"]);
    let broadcast = fixture.call(
        "1",
        json!({"op":"send","to":"*","request_id":"group","payload":null}),
    );
    assert_eq!(broadcast["messages"].as_array().unwrap().len(), 3);

    let now = Instant::now();
    std::thread::scope(|threads| {
        for lane in 0..16 {
            let fixture = &fixture;
            threads.spawn(move || {
                for number in 0..8 {
                    if lane == 15 && number == 7 {
                        break;
                    }
                    let result = fixture.call(
                        "2",
                        json!({"op":"send","to":"3",
                        "request_id":format!("{lane}-{number}"),"payload":number}),
                    );
                    assert_eq!(result["ok"], true, "{result}");
                }
            });
        }
    });
    eprintln!(
        "127 durable sends from 16 concurrent clients: {:?}",
        now.elapsed()
    );
    assert_eq!(fixture.call("3", json!({"op":"inbox"}))["pending"], 128);
    assert_eq!(
        fixture.call(
            "1",
            json!({"op":"send","to":"*","request_id":"full","payload":null})
        )["error"],
        "inbox_full:3"
    );
    assert_eq!(fixture.call("2", json!({"op":"inbox"}))["pending"], 1);
    assert_eq!(fixture.call("4", json!({"op":"inbox"}))["pending"], 1);

    fixture.crash();
    fixture.start();
    assert_eq!(fixture.call("1", send), sent);
    assert_eq!(
        fixture.call(
            "1",
            json!({"op":"send","to":"2","request_id":"task","payload":"changed"})
        )["error"],
        "request_id_conflict"
    );
    assert_eq!(
        fixture.call("1", json!({"op":"receive"}))["messages"][0]["payload"],
        "ok"
    );
    assert_eq!(fixture.call("3", json!({"op":"inbox"}))["pending"], 128);
}

#[test]
fn meeting_ready_barrier_and_summary_recovery() {
    let mut fixture = Fixture::new();
    let create = json!({"op":"meeting_create","meeting_id":"m"});
    assert_eq!(fixture.call("1", create)["phase"], "preparing");
    let status = fixture.call("4", json!({"op":"meeting_status","meeting_id":"m"}));
    assert_eq!(status["participants"].as_array().unwrap().len(), 4);
    let ready = json!({"op":"meeting_ready","meeting_id":"m"});
    assert_eq!(fixture.call("1", ready.clone())["phase"], "preparing");
    assert_eq!(fixture.call("2", ready.clone())["phase"], "preparing");
    assert_eq!(fixture.call("3", ready.clone())["phase"], "preparing");
    let summary = json!({"op":"meeting_summary","meeting_id":"m","payload":null});
    assert_eq!(
        fixture.call("1", summary.clone())["error"],
        "meeting_not_ready"
    );
    fixture.crash();
    fixture.start();

    let exchange = waiting(
        &fixture,
        "2",
        json!({"op":"meeting_status","meeting_id":"m","until":"exchange","wait_ms":2000}),
    );
    assert!(exchange.recv_timeout(Duration::from_millis(30)).is_err());
    assert_eq!(fixture.call("4", ready)["phase"], "exchange");
    assert_eq!(
        exchange.recv_timeout(Duration::from_secs(1)).unwrap()["phase"],
        "exchange"
    );
    let complete = waiting(
        &fixture,
        "3",
        json!({"op":"meeting_status","meeting_id":"m","until":"complete","wait_ms":2000}),
    );
    for id in ["1", "2", "3"] {
        assert_eq!(fixture.call(id, summary.clone())["phase"], "exchange");
    }
    assert!(complete.recv_timeout(Duration::from_millis(30)).is_err());
    assert_eq!(fixture.call("4", summary.clone())["phase"], "complete");
    assert_eq!(
        complete.recv_timeout(Duration::from_secs(1)).unwrap()["phase"],
        "complete"
    );

    fixture.crash();
    fixture.start();
    assert_eq!(fixture.call("1", summary)["phase"], "complete");
    assert_eq!(
        fixture.call(
            "1",
            json!({"op":"meeting_summary","meeting_id":"m","payload":"changed"})
        )["error"],
        "summary_conflict"
    );
    let page = fixture.call(
        "2",
        json!({"op":"meeting_summaries","meeting_id":"m","limit":2}),
    );
    assert_eq!(page["summaries"].as_array().unwrap().len(), 2);
    let final_page = fixture.call("3", json!({"op":"meeting_summaries","meeting_id":"m","after_agent_id":page["next_after_agent_id"]}));
    assert_eq!(final_page["summaries"][0]["agent_id"], "3");
    assert!(final_page["next_after_agent_id"].is_null());

    let restricted = json!({"op":"meeting_create","meeting_id":"small","participants":["1","2"]});
    assert_eq!(fixture.call("1", restricted)["ok"], true);
    assert_eq!(
        fixture.call("4", json!({"op":"meeting_summaries","meeting_id":"small"}))["error"],
        "unknown_meeting"
    );
}

struct Mcp {
    child: Child,
    input: std::process::ChildStdin,
    output: BufReader<std::process::ChildStdout>,
    id: u64,
}

impl Mcp {
    fn new(fixture: &Fixture, agent: &str) -> Self {
        let path = fixture.root.join(format!("agent-{agent}.json"));
        use std::os::unix::fs::OpenOptionsExt;
        let mut credentials = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        write!(
            credentials,
            "{}",
            json!({"agent_id":agent,"token":token(agent)})
        )
        .unwrap();
        let mut child = Command::new(BIN)
            .arg("mcp")
            .env_remove("AGENT_CHANNEL_ID")
            .env_remove("AGENT_CHANNEL_TOKEN")
            .env("AGENT_CHANNEL_STATE_DIR", fixture.root.join("s"))
            .env("AGENT_CHANNEL_CREDENTIALS_FILE", path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut mcp = Self {
            child,
            input,
            output,
            id: 0,
        };
        let initialized = mcp.request("initialize", json!({
            "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}
        }));
        assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
        writeln!(
            mcp.input,
            "{}",
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
        )
        .unwrap();
        mcp
    }

    fn begin(&mut self, method: &str, params: Value) -> u64 {
        self.id += 1;
        writeln!(
            self.input,
            "{}",
            json!({"jsonrpc":"2.0","id":self.id,"method":method,"params":params})
        )
        .unwrap();
        self.id
    }

    fn read(&mut self, id: u64) -> Value {
        let mut line = String::new();
        self.output.read_line(&mut line).unwrap();
        let value: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["id"], id);
        value
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.begin(method, params);
        self.read(id)
    }

    fn call(&mut self, name: &str, arguments: Value) -> Value {
        self.request("tools/call", json!({"name":name,"arguments":arguments}))["result"].clone()
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn mcp_exposes_channel_and_runs_concurrent_calls() {
    let fixture = Fixture::new();
    let mut first = Mcp::new(&fixture, "1");
    let mut second = Mcp::new(&fixture, "2");
    let listed = first.request("tools/list", json!({}));
    assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 10);
    assert_eq!(
        first.call("whoami", json!({}))["structuredContent"]["agent_id"],
        "1"
    );
    assert_eq!(
        second.call("whoami", json!({}))["structuredContent"]["agent_id"],
        "2"
    );
    assert_eq!(
        first.request(
            "tools/call",
            json!({"name":"whoami","arguments":{"agent_id":"2"}})
        )["error"]["code"],
        -32602
    );
    let receive = second.begin(
        "tools/call",
        json!({"name":"receive","arguments":{"wait_ms":4000}}),
    );
    assert_eq!(
        second.call("inbox", json!({}))["structuredContent"]["pending"],
        0
    );
    assert_eq!(
        first.call(
            "send",
            json!({"to":"2","request_id":"plugin-delivery","payload":{"task":"work"}})
        )["isError"],
        false
    );
    let received = second.read(receive);
    let message = &received["result"]["structuredContent"]["messages"][0];
    assert_eq!(message["sender"], "1");
    assert_eq!(message["payload"]["task"], "work");
    assert_eq!(
        second.call("ack", json!({"ids":[message["id"]]}))["isError"],
        false
    );
    assert_eq!(
        first.call(
            "meeting_create",
            json!({"meeting_id":"plugins","participants":["1","2"]})
        )["structuredContent"]["phase"],
        "preparing"
    );
    assert_eq!(
        first.call(
            "meeting_summary",
            json!({"meeting_id":"plugins","payload":"early"})
        )["isError"],
        true
    );
    first.call("meeting_ready", json!({"meeting_id":"plugins"}));
    assert_eq!(
        second.call("meeting_ready", json!({"meeting_id":"plugins"}))["structuredContent"]["phase"],
        "exchange"
    );
    first.call(
        "meeting_summary",
        json!({"meeting_id":"plugins","payload":"from 1"}),
    );
    assert_eq!(
        second.call(
            "meeting_summary",
            json!({"meeting_id":"plugins","payload":"from 2"})
        )["structuredContent"]["phase"],
        "complete"
    );
    assert_eq!(
        first.call("meeting_summaries", json!({"meeting_id":"plugins"}))["structuredContent"]
            ["summaries"][1]["payload"],
        "from 2"
    );
}
