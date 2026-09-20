use serde_json::json;
use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Instant};
use ws::{CloseCode, Error, Handler, Handshake, Message, Result, Sender, util::Token};

const TICK: Token = Token(1);
const TICK_MS: u64 = 20_000;
const IDLE_SECS: u64 = 30;

#[derive(Debug, serde::Deserialize)]
struct Config {
    sig_port: String,
}

struct Client {
    pool: Rc<RefCell<HashMap<String, Sender>>>,
    id: Option<String>,
    last_seen: Instant,
    out: Sender,
}

impl Handler for Client {
    fn on_open(&mut self, _: Handshake) -> Result<()> {
        eprintln!("[open] conn={}", self.out.connection_id());
        self.out.timeout(TICK_MS, TICK)
    }

    fn on_timeout(&mut self, event: Token) -> Result<()> {
        if event != TICK {
            return Ok(());
        }
        if self.last_seen.elapsed().as_secs() > IDLE_SECS {
            eprintln!(
                "[idle] conn={} silent for {}s, closing",
                self.out.connection_id(),
                self.last_seen.elapsed().as_secs()
            );
            self.cleanup();
            return self.out.close(CloseCode::Away);
        }
        self.out.send(r#"{"ping":true}"#)?;
        self.out.timeout(TICK_MS, TICK)
    }

    fn on_message(&mut self, msg: Message) -> Result<()> {
        self.last_seen = Instant::now();
        let text = match msg {
            Message::Text(t) => t,
            _ => return Ok(()),
        };

        // Invalid JSON
        let v: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "[error] conn={} invalid_json: {e} | {}",
                    self.out.connection_id(),
                    text
                );
                let _ = self
                    .out
                    .send(json!({ "error": "invalid_json" }).to_string());
                return Ok(());
            }
        };

        if v.get("pong").is_some() {
            return Ok(());
        }

        // register
        if let Some(id) = v.get("register").and_then(|x| x.as_str()) {
            let id = id.to_string();
            let force = v.get("force").and_then(|x| x.as_bool()).unwrap_or(false);

            if self.pool.borrow().contains_key(&id) && !force {
                eprintln!(
                    "[reg] {id} denied: id_taken (conn={})",
                    self.out.connection_id()
                );
                let _ = self
                    .out
                    .send(json!({ "error": "id_taken", "id": id }).to_string());
                return Ok(());
            }

            if force {
                if let Some(sender) = self.pool.borrow().get(&id).cloned() {
                    eprintln!(
                        "[kick] {id} conn={} replaced by conn={}",
                        sender.connection_id(),
                        self.out.connection_id()
                    );
                    let _ = sender.close(CloseCode::Normal);
                }
            }

            self.id = Some(id.clone());
            self.pool.borrow_mut().insert(id.clone(), self.out.clone());
            let online: Vec<String> = self.pool.borrow().keys().cloned().collect();
            eprintln!(
                "[reg] {id} conn={} online={} {online:?}",
                self.out.connection_id(),
                online.len()
            );
            return Ok(());
        }

        // query registration status
        if let Some(id) = v.get("query").and_then(|x| x.as_str()) {
            let registered = self.pool.borrow().contains_key(id);
            eprintln!(
                "[query] {id} -> registered={registered} (conn={})",
                self.out.connection_id()
            );
            let _ = self
                .out
                .send(format!(r#"{{"id":"{}","registered":{}}}"#, id, registered));
            return Ok(());
        }

        // forward
        let from = match v.get("from").and_then(|x| x.as_str()) {
            Some(f) => f,
            None => {
                eprintln!(
                    "[error] conn={} missing from | {text}",
                    self.out.connection_id()
                );
                let _ = self
                    .out
                    .send(json!({ "error": "missing_field", "field": "from" }).to_string());
                return Ok(());
            }
        };
        let to = match v.get("to").and_then(|x| x.as_str()) {
            Some(t) => t,
            None => {
                eprintln!(
                    "[error] conn={} missing to | {text}",
                    self.out.connection_id()
                );
                let _ = self
                    .out
                    .send(json!({ "error": "missing_field", "field": "to" }).to_string());
                return Ok(());
            }
        };
        let kind = v.get("type").and_then(|x| x.as_str()).unwrap_or("-");

        if self.pool.borrow().get(from).is_none() {
            eprintln!(
                "[error] {from} unregistered (conn={})",
                self.out.connection_id()
            );
            let _ = self
                .out
                .send(json!({ "error": "unregistered", "from": from }).to_string());
            return Ok(());
        }
        if let Some(target) = self.pool.borrow().get(to).cloned() {
            eprintln!("[fwd] {from} -> {to} type={kind} {}B", text.len());
            target.send(text)?;
        } else {
            eprintln!("[fwd] {from} -> {to} type={kind} offline");
            let _ = self
                .out
                .send(json!({ "error": "offline", "to": to }).to_string());
        }

        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        eprintln!(
            "[close] conn={} code={code:?} reason={reason:?}",
            self.out.connection_id()
        );
        self.cleanup();
    }

    fn on_error(&mut self, err: Error) {
        eprintln!("[error] conn={} {err}", self.out.connection_id());
        self.cleanup();
    }
}

impl Client {
    fn cleanup(&self) {
        if let Some(ref id) = self.id {
            let mut pool = self.pool.borrow_mut();
            // only remove the entry if it's still the same sender
            let mine = pool
                .get(id)
                .is_some_and(|s| s.connection_id() == self.out.connection_id());
            if mine {
                pool.remove(id);
                let online: Vec<String> = pool.keys().cloned().collect();
                eprintln!(
                    "[off] {id} conn={} online={} {online:?}",
                    self.out.connection_id(),
                    online.len()
                );
            }
        }
    }
}

fn main() {
    let pool = Rc::new(RefCell::new(HashMap::<String, Sender>::new()));

    let content = std::fs::read_to_string("config.yaml").expect("cannot find config.yaml");
    let config: Config = serde_yaml::from_str(&content).expect("not valid yaml file");

    let ws = ws::Builder::new()
        .build({
            let pool = pool.clone();
            move |out: Sender| Client {
                pool: pool.clone(),
                id: None,
                last_seen: Instant::now(),
                out,
            }
        })
        .unwrap();

    ws.listen(format!("0.0.0.0:{}", config.sig_port)).unwrap();
    eprintln!("Server listening on port {}", config.sig_port);
}
