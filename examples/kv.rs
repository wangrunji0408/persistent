use persistent::{Persist, Persistable};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Default, Debug, Serialize, Deserialize)]
struct KV {
    kv: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Op {
    Insert { key: String, value: String },
    Remove { key: String },
}

impl Persistable for KV {
    type Operation = Op;
    type ApplyResult = ();

    fn apply(&mut self, op: Self::Operation) -> Self::ApplyResult {
        match op {
            Op::Insert { key, value } => {
                self.kv.insert(key, value);
            }
            Op::Remove { key } => {
                self.kv.remove(&key);
            }
        }
    }
}

fn main() {
    env_logger::init();
    let mut kv = Persist::<KV>::open_or_create("data").unwrap();
    kv.apply(Op::Insert {
        key: "Alice".into(),
        value: "1".into(),
    })
    .unwrap();
    kv.apply(Op::Insert {
        key: "Bob".into(),
        value: "2".into(),
    })
    .unwrap();
    kv.apply(Op::Remove { key: "Bob".into() }).unwrap();
    kv.snapshot().unwrap();
}
