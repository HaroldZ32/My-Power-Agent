use crate::{failure, MainResult, FRAME};
use serde_json::{json, Value};
use std::fs::File;
use std::io::{self, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{self as async_io, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

const VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const MAX_RESPONSE: u64 = 2 * 1024 * 1024;
const TOOL_WAIT: Duration = Duration::from_secs(70);

fn tool(
    name: &str,
    description: &str,
    required: &[&str],
    properties: Value,
    read_only: bool,
) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": {"type":"object", "required":required, "properties":properties, "additionalProperties":false},
        "annotations": {"readOnlyHint":read_only, "idempotentHint":!read_only, "openWorldHint":false}
    })
}

fn tools() -> Vec<Value> {
    let agent = json!({"type":"string","pattern":"^[1-9][0-9]{0,17}$"});
    let message = json!({"type":"integer","minimum":1});
    let name = json!({"type":"string","minLength":1,"maxLength":128});
    let limit = json!({"type":"integer","minimum":1,"maximum":32});
    let wait = json!({"type":"integer","minimum":0,"maximum":60000});
    let meeting = json!({"meeting_id":name});
    vec![
        tool("whoami", "Get this agent's fixed channel ID.", &[], json!({}), true),
        tool("inbox", "Count this agent's unacknowledged messages.", &[], json!({}), true),
        tool("send", "Durably send to an agent ID or broadcast to all other IDs with *. Reuse the same request_id and content when retrying.", &["to","request_id","payload"],
            json!({"to":{"anyOf":[agent,{"const":"*"}]},"request_id":name,"payload":{},"reply_to":message}), false),
        tool("receive", "Read unacknowledged messages. Optionally wait for an arrival or paginate after an ID. Acknowledge after processing.", &[],
            json!({"wait_ms":wait,"limit":limit,"after_id":{"type":"integer","minimum":0}}), true),
        tool("ack", "Acknowledge messages in this agent's inbox after processing.", &["ids"],
            json!({"ids":{"type":"array","items":message,"minItems":1,"maxItems":32}}), false),
        tool("meeting_create", "Create a meeting with a fixed participant list. Omit participants to include the whole roster.", &["meeting_id"],
            json!({"meeting_id":name,"participants":{"type":"array","items":agent,"minItems":2,"maxItems":1024,"uniqueItems":true}}), false),
        tool("meeting_ready", "Report this agent ready for a meeting.", &["meeting_id"], meeting, false),
        tool("meeting_status", "Read meeting status. Wait with until=exchange for everyone to be ready, or until=complete for all summaries. On timeout inspect phase.", &["meeting_id"],
            json!({"meeting_id":name,"until":{"enum":["exchange","complete"]},"wait_ms":wait}), true),
        tool("meeting_summary", "Publish this agent's summary once everyone is ready. Retry with identical content.", &["meeting_id","payload"],
            json!({"meeting_id":name,"payload":{}}), false),
        tool("meeting_summaries", "Read summaries published by participants. Use next_after_agent_id to get another page.", &["meeting_id"],
            json!({"meeting_id":name,"limit":limit,"after_agent_id":agent}), true),
    ]
}

struct Client {
    socket: PathBuf,
    agent: Option<String>,
    token: Option<String>,
}

impl Client {
    async fn call(&self, operation: &str, mut arguments: Value) -> Value {
        let (Some(agent), Some(token)) = (&self.agent, &self.token) else {
            return failure("missing_agent_channel_credentials");
        };
        arguments["op"] = operation.into();
        arguments["agent_id"] = agent.as_str().into();
        arguments["token"] = token.as_str().into();
        let mut bytes = arguments.to_string().into_bytes();
        bytes.push(b'\n');
        if bytes.len() > FRAME {
            return failure("frame_too_large");
        }
        match tokio::time::timeout(TOOL_WAIT, self.exchange(&bytes)).await {
            Ok(Ok(value)) => value,
            Ok(Err(_)) => failure("channel_unavailable"),
            Err(_) => failure("channel_timeout"),
        }
    }

    async fn exchange(&self, bytes: &[u8]) -> io::Result<Value> {
        let mut stream = UnixStream::connect(&self.socket).await?;
        stream.write_all(bytes).await?;
        let mut response = Vec::new();
        BufReader::new(stream)
            .take(MAX_RESPONSE)
            .read_until(b'\n', &mut response)
            .await?;
        if !response.ends_with(b"\n") {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete channel response",
            ));
        }
        Ok(serde_json::from_slice(&response)?)
    }
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":result})
}

fn rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}

type Output = Arc<Mutex<tokio::io::Stdout>>;

async fn reply(output: &Output, value: Value) -> io::Result<()> {
    let mut bytes = value.to_string().into_bytes();
    bytes.push(b'\n');
    let mut out = output.lock().await;
    out.write_all(&bytes).await?;
    out.flush().await
}

pub async fn serve(directory: &Path) -> MainResult<()> {
    let (agent, token) = credentials()?;
    let client = Arc::new(Client {
        socket: directory.join("channel.sock"),
        agent,
        token,
    });
    let output = Arc::new(Mutex::new(async_io::stdout()));
    let tools = tools();
    let mut input = BufReader::new(async_io::stdin());
    let mut frame = Vec::new();
    let mut tasks = JoinSet::new();
    let slots = Arc::new(Semaphore::new(32));
    let mut initialized = false;
    loop {
        while let Some(result) = tasks.try_join_next() {
            result??;
        }
        frame.clear();
        let size = (&mut input)
            .take((FRAME + 1) as u64)
            .read_until(b'\n', &mut frame)
            .await?;
        if size == 0 {
            break;
        }
        if size > FRAME {
            reply(&output, rpc_error(Value::Null, -32600, "Request too large")).await?;
            break;
        }
        let Ok(request) = serde_json::from_slice::<Value>(&frame) else {
            reply(&output, rpc_error(Value::Null, -32700, "Parse error")).await?;
            continue;
        };
        let id = request["id"].clone();
        if request["jsonrpc"] != "2.0" || !request["method"].is_string() {
            reply(&output, rpc_error(id, -32600, "Invalid request")).await?;
            continue;
        }
        if request.get("id").is_none() {
            continue;
        }
        let params = &request["params"];
        let response = match request["method"].as_str().unwrap() {
            "initialize" => {
                let Some(version) = params["protocolVersion"].as_str() else {
                    reply(
                        &output,
                        rpc_error(id, -32602, "protocolVersion is required"),
                    )
                    .await?;
                    continue;
                };
                let version = if VERSIONS.contains(&version) {
                    version
                } else {
                    VERSIONS[0]
                };
                initialized = true;
                rpc_result(
                    id,
                    json!({"protocolVersion":version, "capabilities":{"tools":{}},
                    "serverInfo":{"name":"agent-channel","version":env!("CARGO_PKG_VERSION")}}),
                )
            }
            "ping" => rpc_result(id, json!({})),
            _ if !initialized => rpc_error(id, -32002, "Server not initialized"),
            "tools/list" => rpc_result(id, json!({"tools":tools})),
            "tools/call" => {
                let name = params["name"].as_str().unwrap_or_default();
                let args = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let definition = tools.iter().find(|tool| tool["name"] == name);
                if let Some((definition, args)) = definition.zip(args.as_object()) {
                    if !args
                        .keys()
                        .all(|key| definition["inputSchema"]["properties"].get(key).is_some())
                    {
                        reply(&output, rpc_error(id, -32602, "Unexpected tool argument")).await?;
                        continue;
                    }
                } else {
                    reply(
                        &output,
                        rpc_error(id, -32602, "Unknown tool or invalid arguments"),
                    )
                    .await?;
                    continue;
                }
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    reply(
                        &output,
                        rpc_error(id, -32000, "Too many in-flight tool calls"),
                    )
                    .await?;
                    continue;
                };
                let (name, client, output) = (name.to_owned(), client.clone(), output.clone());
                tasks.spawn(async move {
                    let _permit = permit;
                    let result = client.call(&name, args).await;
                    let response = rpc_result(
                        id,
                        json!({
                            "isError":result["ok"] != true,
                            "content":[{"type":"text","text":result.to_string()}],
                            "structuredContent":result,
                        }),
                    );
                    reply(&output, response).await
                });
                continue;
            }
            _ => rpc_error(id, -32601, "Method not found"),
        };
        reply(&output, response).await?;
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

fn credentials() -> MainResult<(Option<String>, Option<String>)> {
    let agent = std::env::var("AGENT_CHANNEL_ID").ok();
    let token = std::env::var("AGENT_CHANNEL_TOKEN").ok();
    if agent.is_some() && token.is_some() {
        return Ok((agent, token));
    }
    let Some(path) =
        std::env::var_os("AGENT_CHANNEL_CREDENTIALS_FILE").filter(|path| !path.is_empty())
    else {
        return Ok((agent, token));
    };
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(
            "credential file must be a private regular file owned by the current user".into(),
        );
    }
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err("credential file exceeds 4 KiB".into());
    }
    let value: Value = serde_json::from_slice(&bytes)?;
    let agent = value["agent_id"]
        .as_str()
        .ok_or("credential file is missing agent_id")?;
    let token = value["token"]
        .as_str()
        .ok_or("credential file is missing token")?;
    Ok((Some(agent.into()), Some(token.into())))
}
