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
        self.out.timeout(TICK_MS, TICK)
    }

    fn on_timeout(&mut self, event: Token) -> Result<()> {
        if event != TICK {
            return Ok(());
        }
        if self.last_seen.elapsed().as_secs() > IDLE_SECS {
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
            Err(_) => {
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
                let _ = self
                    .out
                    .send(json!({ "error": "id_taken", "id": id }).to_string());
                return Ok(());
            }

            if force {
                if let Some(sender) = self.pool.borrow().get(&id).cloned() {
                    let _ = sender.close(CloseCode::Normal);
                }
            }

            self.id = Some(id.clone());
            self.pool.borrow_mut().insert(id, self.out.clone());
            return Ok(());
        }

        // query registration status
        if let Some(id) = v.get("query").and_then(|x| x.as_str()) {
            let registered = self.pool.borrow().contains_key(id);
            let _ = self
                .out
                .send(format!(r#"{{"id":"{}","registered":{}}}"#, id, registered));
            return Ok(());
        }

        // forward
        let from = match v.get("from").and_then(|x| x.as_str()) {
            Some(f) => f,
            None => {
                let _ = self
                    .out
                    .send(json!({ "error": "missing_field", "field": "from" }).to_string());
                return Ok(());
            }
        };
        let to = match v.get("to").and_then(|x| x.as_str()) {
            Some(t) => t,
            None => {
                let _ = self
                    .out
                    .send(json!({ "error": "missing_field", "field": "to" }).to_string());
                return Ok(());
            }
        };

        if self.pool.borrow().get(from).is_none() {
            let _ = self
                .out
                .send(json!({ "error": "unregistered", "from": from }).to_string());
            return Ok(());
        }
        if let Some(target) = self.pool.borrow().get(to).cloned() {
            target.send(text)?;
        } else {
            let _ = self
                .out
                .send(json!({ "error": "offline", "to": to }).to_string());
        }

        Ok(())
    }

    fn on_close(&mut self, _: CloseCode, _: &str) {
        self.cleanup();
    }

    fn on_error(&mut self, _: Error) {
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
