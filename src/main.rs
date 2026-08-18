use std::{cell::RefCell, collections::HashMap, rc::Rc};
use ws::{CloseCode, Error, Handler, Message, Result, Sender};

#[derive(Debug, serde::Deserialize)]
struct Config {
    sig_port: String,
}

struct Client {
    pool: Rc<RefCell<HashMap<String, Sender>>>,
    id: Option<String>,
    out: Sender,
}

impl Handler for Client {
    fn on_message(&mut self, msg: Message) -> Result<()> {
        let text = match msg {
            Message::Text(t) => t,
            _ => return Ok(()),
        };

        // Invalid JSON
        let v: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => {
                let _ = self.out.send("Invalid JSON");
                return Ok(());
            }
        };

        // register
        if let Some(id) = v.get("register").and_then(|x| x.as_str()) {
            let id = id.to_string();
            self.id = Some(id.clone());

            let old_sender = self.pool.borrow().get(&id).cloned();
            if let Some(sender) = old_sender {
                let _ = sender.close(CloseCode::Normal);
            }

            self.pool.borrow_mut().insert(id, self.out.clone());
            return Ok(());
        }

        // forward
        let from = match v.get("from").and_then(|x| x.as_str()) {
            Some(f) => f,
            None => {
                let _ = self.out.send("Missing `from`");
                return Ok(());
            }
        };
        let to = match v.get("to").and_then(|x| x.as_str()) {
            Some(t) => t,
            None => {
                let _ = self.out.send("Missing `to`");
                return Ok(());
            }
        };

        if self.pool.borrow().get(from).is_none() {
            let _ = self.out.send(format!("{} not registered", from));
            return Ok(());
        }
        if let Some(target) = self.pool.borrow().get(to).cloned() {
            target.send(text)?;
        } else {
            let _ = self.out.send(format!("{} offline", to));
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
            self.pool.borrow_mut().remove(id);
        }
    }
}

fn main() {
    let pool = Rc::new(RefCell::new(HashMap::<String, Sender>::new()));

    let content = std::fs::read_to_string("config.yaml").expect("read config");
    let config: Config = serde_yaml::from_str(&content).expect("parse config");

    let ws = ws::Builder::new()
        .build({
            let pool = pool.clone();
            move |out: Sender| Client {
                pool: pool.clone(),
                id: None,
                out,
            }
        })
        .unwrap();

    ws.listen(format!("0.0.0.0:{}", config.sig_port)).unwrap();
    eprintln!("Server listening on port {}", config.sig_port);
}
