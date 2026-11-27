use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub enum Error {
    Request(String),
    Storage(rusqlite::Error),
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn invalid<T>(code: &str) -> Result<T> {
    Err(Error::Request(code.into()))
}

pub fn integer(value: Option<&Value>, min: i64, max: i64, error: &str) -> Result<i64> {
    match value.and_then(Value::as_i64) {
        Some(n) if (min..=max).contains(&n) => Ok(n),
        _ => invalid(error),
    }
}

pub fn name<'a>(value: Option<&'a Value>, error: &str) -> Result<&'a str> {
    match value.and_then(Value::as_str) {
        Some(s) if !s.is_empty() && s.len() <= 128 => Ok(s),
        _ => invalid(error),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn payload(value: Option<&Value>) -> Result<String> {
    match value.map(Value::to_string) {
        Some(s) if s.len() <= 32 * 1024 => Ok(s),
        _ => invalid("invalid_payload"),
    }
}

fn stored_json(s: String) -> rusqlite::Result<Value> {
    serde_json::from_str(&s).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

pub struct Store {
    db: Connection,
    capacity: i64,
    roster: BTreeSet<String>,
}

#[derive(Clone, Copy)]
pub enum Wake {
    Mail,
    Exchange,
    Complete,
}

pub type Outcome = (Value, Option<(Wake, Vec<String>)>);

impl Store {
    pub fn open(path: &Path, capacity: i64, roster: BTreeSet<String>) -> rusqlite::Result<Self> {
        let db = Connection::open(path)?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS submissions (
                 id INTEGER PRIMARY KEY, sender TEXT NOT NULL, request_id TEXT NOT NULL,
                 destination TEXT NOT NULL, payload TEXT NOT NULL, reply_to INTEGER,
                 thread_id INTEGER, created_ms INTEGER NOT NULL, UNIQUE(sender, request_id)
             );
             CREATE TABLE IF NOT EXISTS messages (
                 id INTEGER PRIMARY KEY, submission INTEGER NOT NULL REFERENCES submissions(id),
                 recipient TEXT NOT NULL, read_ms INTEGER
             );
             CREATE INDEX IF NOT EXISTS pending ON messages(recipient, id) WHERE read_ms IS NULL;
             CREATE INDEX IF NOT EXISTS submitted ON messages(submission, id);
             CREATE TABLE IF NOT EXISTS meetings (
                 id TEXT PRIMARY KEY, creator TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1
             );
             CREATE TABLE IF NOT EXISTS attendees (
                 meeting TEXT NOT NULL REFERENCES meetings(id), agent TEXT NOT NULL,
                 ready INTEGER NOT NULL DEFAULT 0, summary TEXT,
                 PRIMARY KEY(meeting, agent)
             );
             CREATE INDEX IF NOT EXISTS meeting_unready ON attendees(meeting) WHERE ready=0;
             CREATE INDEX IF NOT EXISTS meeting_unsubmitted ON attendees(meeting) WHERE summary IS NULL;",
        )?;
        Ok(Self {
            db,
            capacity,
            roster,
        })
    }

    pub fn execute(&mut self, agent: &str, req: &Value) -> Result<Outcome> {
        match req["op"].as_str() {
            Some("send") => self.send(agent, req),
            Some("receive") => Ok((self.receive(agent, req)?, None)),
            Some("inbox") => {
                let pending: i64 = self.db.query_row(
                    "SELECT count(*) FROM messages WHERE recipient=? AND read_ms IS NULL",
                    [agent],
                    |row| row.get(0),
                )?;
                Ok((
                    json!({"agent_id": agent, "pending": pending, "capacity": self.capacity}),
                    None,
                ))
            }
            Some("ack") => Ok((self.ack(agent, req)?, None)),
            Some(
                "meeting_create" | "meeting_ready" | "meeting_summary" | "meeting_status"
                | "meeting_summaries",
            ) => self.meeting(agent, req),
            _ => invalid("unknown_operation"),
        }
    }

    fn send(&mut self, agent: &str, req: &Value) -> Result<Outcome> {
        let destination = name(req.get("to"), "unknown_recipient")?;
        let key = name(req.get("request_id"), "invalid_request_id")?;
        let content = payload(req.get("payload"))?;
        let reply = req
            .get("reply_to")
            .map(|value| integer(Some(value), 1, i64::MAX, "invalid_reply_to"))
            .transpose()?;
        let tx = self.db.transaction()?;
        let prior = tx
            .query_row(
                "SELECT id, destination, payload, reply_to, coalesce(thread_id, id)
             FROM submissions WHERE sender=? AND request_id=?",
                params![agent, key],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?;
        if let Some((id, previous_to, previous_content, previous_reply, thread)) = prior {
            if (destination, &content, reply)
                != (previous_to.as_str(), &previous_content, previous_reply)
            {
                return invalid("request_id_conflict");
            }
            let mut query = tx.prepare_cached(
                "SELECT id, recipient FROM messages WHERE submission=? ORDER BY id",
            )?;
            let rows = query
                .query_map([id], |r| {
                    Ok(json!({"id": r.get::<_, i64>(0)?, "recipient": r.get::<_, String>(1)?}))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            return Ok((json!({"thread_id": thread, "messages": rows}), None));
        }
        let targets: Vec<String> = if destination == "*" {
            self.roster
                .iter()
                .filter(|id| id.as_str() != agent)
                .cloned()
                .collect()
        } else if destination != agent && self.roster.contains(destination) {
            vec![destination.into()]
        } else {
            vec![]
        };
        if targets.is_empty() {
            return invalid("unknown_recipient");
        }
        let thread: Option<i64> = match reply {
            Some(id) if destination != "*" => {
                let thread = tx.query_row(
                    "SELECT coalesce(s.thread_id, s.id) FROM messages m JOIN submissions s ON s.id=m.submission
                     WHERE m.id=? AND m.recipient=? AND s.sender=?", params![id, agent, destination],
                    |row| row.get(0),
                ).optional()?;
                if thread.is_none() {
                    return invalid("invalid_reply_to");
                }
                thread
            }
            Some(_) => return invalid("invalid_reply_to"),
            None => None,
        };
        {
            let mut check = tx.prepare_cached(
                "SELECT 1 FROM messages WHERE recipient=? AND read_ms IS NULL LIMIT 1 OFFSET ?",
            )?;
            for target in &targets {
                if check.exists(params![target, self.capacity - 1])? {
                    return invalid(&format!("inbox_full:{target}"));
                }
            }
        }
        tx.execute(
            "INSERT INTO submissions(sender, request_id, destination, payload, reply_to, thread_id, created_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)", params![agent, key, destination, content, reply, thread, now_ms()],
        )?;
        let id = tx.last_insert_rowid();
        let mut messages = Vec::with_capacity(targets.len());
        {
            let mut insert =
                tx.prepare_cached("INSERT INTO messages(submission, recipient) VALUES (?, ?)")?;
            for target in &targets {
                insert.execute(params![id, target])?;
                messages.push(json!({"id": tx.last_insert_rowid(), "recipient": target}));
            }
        }
        tx.commit()?;
        Ok((
            json!({"thread_id": thread.unwrap_or(id), "messages": messages}),
            Some((Wake::Mail, targets)),
        ))
    }

    fn receive(&self, agent: &str, req: &Value) -> Result<Value> {
        let limit = integer(
            Some(req.get("limit").unwrap_or(&json!(16))),
            1,
            32,
            "invalid_limit",
        )?;
        let after = match req.get("after_id") {
            Some(value) => integer(Some(value), 0, i64::MAX, "invalid_after_id")?,
            None => 0,
        };
        let mut query = self.db.prepare_cached(
            "SELECT m.id, s.sender, coalesce(s.thread_id, s.id), s.reply_to, s.payload, s.created_ms
             FROM messages m JOIN submissions s ON s.id=m.submission
             WHERE m.recipient=? AND m.id>? AND m.read_ms IS NULL ORDER BY m.id LIMIT ?",
        )?;
        let rows = query.query_map(params![agent, after, limit], |r| {
            Ok(json!({"id":r.get::<_, i64>(0)?, "sender":r.get::<_, String>(1)?, "recipient":agent,
                "thread_id":r.get::<_, i64>(2)?, "reply_to":r.get::<_, Option<i64>>(3)?,
                "payload":stored_json(r.get(4)?)?, "created_ms":r.get::<_, i64>(5)?}))
        })?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(json!({"messages": rows}))
    }

    fn ack(&mut self, agent: &str, req: &Value) -> Result<Value> {
        let Some(ids) = req["ids"]
            .as_array()
            .filter(|ids| (1..=32).contains(&ids.len()))
        else {
            return invalid("invalid_ids");
        };
        let tx = self.db.transaction()?;
        let now = now_ms();
        {
            let mut update = tx.prepare_cached(
                "UPDATE messages SET read_ms=coalesce(read_ms, ?) WHERE id=? AND recipient=?",
            )?;
            for item in ids {
                let id = integer(Some(item), 1, i64::MAX, "invalid_ids")?;
                if update.execute(params![now, id, agent])? == 0 {
                    return invalid("unknown_message");
                }
            }
        }
        tx.commit()?;
        Ok(json!({"acked": ids}))
    }

    fn meeting(&mut self, agent: &str, req: &Value) -> Result<Outcome> {
        let id = name(req.get("meeting_id"), "invalid_meeting_id")?;
        let op = req["op"].as_str().unwrap();
        let members = if op == "meeting_create" {
            Some(self.participants(agent, req.get("participants"))?)
        } else {
            None
        };
        let tx = self.db.transaction()?;
        let existing = tx
            .query_row(
                "SELECT creator, revision FROM meetings WHERE id=?",
                [id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()?;
        let mut wake = None;
        let mut revision;
        if let Some(members) = members {
            if let Some((owner, previous)) = existing {
                let mut query = tx.prepare_cached("SELECT agent FROM attendees WHERE meeting=?")?;
                let current = query
                    .query_map([id], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<BTreeSet<_>>>()?;
                if owner != agent || current != members {
                    return invalid("meeting_id_conflict");
                }
                revision = previous;
            } else {
                tx.execute(
                    "INSERT INTO meetings(id, creator) VALUES (?, ?)",
                    params![id, agent],
                )?;
                let mut insert =
                    tx.prepare_cached("INSERT INTO attendees(meeting, agent) VALUES (?, ?)")?;
                for member in members {
                    insert.execute(params![id, member])?;
                }
                revision = 1;
            }
        } else {
            let Some((_, previous)) = existing else {
                return invalid("unknown_meeting");
            };
            revision = previous;
            let member = tx
                .query_row(
                    "SELECT ready, CASE WHEN ?3 THEN summary END FROM attendees
                     WHERE meeting=?1 AND agent=?2",
                    params![id, agent, op == "meeting_summary"],
                    |r| Ok((r.get::<_, bool>(0)?, r.get::<_, Option<String>>(1)?)),
                )
                .optional()?;
            let Some((ready, previous_summary)) = member else {
                return invalid("unknown_meeting");
            };
            if op == "meeting_ready" && !ready {
                tx.execute(
                    "UPDATE attendees SET ready=1 WHERE meeting=? AND agent=?",
                    params![id, agent],
                )?;
                revision += 1;
                if meeting_phase(&tx, id)? == "exchange" {
                    wake = Some(Wake::Exchange);
                }
            }
            if op == "meeting_summary" {
                let content = payload(req.get("payload"))?;
                if meeting_phase(&tx, id)? == "preparing" {
                    return invalid("meeting_not_ready");
                }
                match previous_summary {
                    Some(previous) if previous != content => return invalid("summary_conflict"),
                    Some(_) => {}
                    None => {
                        tx.execute(
                            "UPDATE attendees SET summary=? WHERE meeting=? AND agent=?",
                            params![content, id, agent],
                        )?;
                        revision += 1;
                        if meeting_phase(&tx, id)? == "complete" {
                            wake = Some(Wake::Complete);
                        }
                    }
                }
            }
            if revision != previous {
                tx.execute(
                    "UPDATE meetings SET revision=? WHERE id=?",
                    params![revision, id],
                )?;
            }
        }
        let phase = meeting_phase(&tx, id)?;
        let mut result = json!({"meeting_id": id, "revision": revision, "phase": phase});
        if op == "meeting_status" && req.get("until").is_none() {
            let mut query = tx.prepare_cached(
                "SELECT agent, ready, summary IS NOT NULL FROM attendees WHERE meeting=? ORDER BY agent",
            )?;
            let participants = query
                .query_map([id], |r| {
                    Ok(
                        json!({"agent_id":r.get::<_, String>(0)?, "ready":r.get::<_, bool>(1)?,
                    "submitted":r.get::<_, bool>(2)?}),
                    )
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            result["participants"] = participants.into();
        }
        if op == "meeting_summaries" {
            let after = req
                .get("after_agent_id")
                .map(|v| name(Some(v), "invalid_after_agent_id"))
                .transpose()?
                .unwrap_or("");
            let limit = match req.get("limit") {
                Some(value) => integer(Some(value), 1, 32, "invalid_limit")?,
                None => 16,
            };
            let mut query = tx.prepare_cached(
                "SELECT agent, summary FROM attendees
                 WHERE meeting=? AND agent>? AND summary IS NOT NULL ORDER BY agent LIMIT ?",
            )?;
            let mut summaries = query.query_map(params![id, after, limit + 1], |r| {
                Ok(json!({"agent_id":r.get::<_, String>(0)?, "payload":stored_json(r.get(1)?)?}))
            })?.collect::<rusqlite::Result<Vec<_>>>()?;
            if summaries.len() > limit as usize {
                summaries.pop();
                result["next_after_agent_id"] = summaries.last().unwrap()["agent_id"].clone();
            } else {
                result["next_after_agent_id"] = Value::Null;
            }
            result["summaries"] = summaries.into();
        }
        let targets = if let Some(event) = wake {
            let mut query = tx.prepare_cached("SELECT agent FROM attendees WHERE meeting=?")?;
            let agents = query
                .query_map([id], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Some((event, agents))
        } else {
            None
        };
        tx.commit()?;
        Ok((result, targets))
    }

    fn participants(&self, agent: &str, input: Option<&Value>) -> Result<BTreeSet<String>> {
        let Some(input) = input else {
            if self.roster.len() < 2 {
                return invalid("invalid_participants");
            }
            return Ok(self.roster.clone());
        };
        let Some(ids) = input
            .as_array()
            .filter(|ids| (2..=self.roster.len()).contains(&ids.len()))
        else {
            return invalid("invalid_participants");
        };
        let mut members = BTreeSet::new();
        for value in ids {
            match value.as_str() {
                Some(id) if self.roster.contains(id) && members.insert(id.to_owned()) => {}
                _ => return invalid("invalid_participants"),
            }
        }
        if !members.contains(agent) {
            return invalid("invalid_participants");
        }
        Ok(members)
    }
}

fn meeting_phase(db: &Connection, id: &str) -> Result<&'static str> {
    let mut flags = db.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM attendees WHERE meeting=?1 AND ready=0),
                EXISTS(SELECT 1 FROM attendees WHERE meeting=?1 AND summary IS NULL)",
    )?;
    let (unready, unsubmitted): (bool, bool) =
        flags.query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))?;
    Ok(match (unready, unsubmitted) {
        (true, _) => "preparing",
        (false, true) => "exchange",
        (false, false) => "complete",
    })
}
