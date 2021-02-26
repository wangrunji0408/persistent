#[macro_use]
extern crate log;

use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Result, Write},
    path::{Path, PathBuf},
};

pub trait Persistable: Default + Serialize + for<'de> Deserialize<'de> {
    type Operation: Serialize + for<'de> Deserialize<'de>;
    type ApplyResult;

    fn apply(&mut self, op: Self::Operation) -> Self::ApplyResult;
}

pub struct Persist<T: Persistable> {
    state: T,
    path: PathBuf,
    log_file: File,
}

impl<T: Persistable> Persist<T> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!("opening from {:?}", path);
        // open logs
        let log_content = std::fs::read_to_string(path.join("log"))?;
        let logs: Vec<_> = log_content.split("\n").filter(|l| !l.is_empty()).collect();
        // find last checkpoint
        let checkpoint = logs
            .iter()
            .enumerate()
            .rfind(|(_, log)| log.starts_with("checkpoint"));
        let mut state = match checkpoint {
            Some((_, name)) => {
                // load checkpoint
                info!("load checkpoint {:?}", name);
                let checkpoint_file = File::open(path.join(name))?;
                serde_json::from_reader(checkpoint_file)?
            }
            None => T::default(),
        };
        // replay redo logs
        let replay_start_idx = match checkpoint {
            Some((idx, _)) => idx + 1,
            None => 0,
        };
        info!("replay {} logs", logs.len() - replay_start_idx);
        for line in &logs[replay_start_idx..] {
            let op = serde_json::from_str(line)?;
            state.apply(op);
        }
        // construct
        Ok(Persist {
            state,
            path: path.into(),
            log_file: OpenOptions::new()
                .create(true)
                .append(true)
                .open(path.join("log"))?,
        })
    }

    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!("creating at {:?}", path);
        std::fs::create_dir_all(path)?;
        Ok(Persist {
            state: T::default(),
            path: path.into(),
            log_file: OpenOptions::new()
                .create(true)
                .append(true)
                .open(path.join("log"))?,
        })
    }

    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self> {
        match Self::open(path.as_ref()) {
            Err(e) if e.kind() == ErrorKind::NotFound => Self::create(path),
            x => x,
        }
    }

    pub fn snapshot(&mut self) -> Result<()> {
        let filename = format!("checkpoint.{:?}", chrono::Local::now());
        let mut file = File::create(self.path.join(&filename))?;
        serde_json::to_writer(&mut file, &self.state)?;
        file.sync_all()?;
        writeln!(&mut self.log_file, "{}", filename)?;
        self.log_file.sync_all()?; // commit point
        info!("create snapshot at {:?}", filename);
        Ok(())
    }

    pub fn apply(&mut self, op: T::Operation) -> Result<T::ApplyResult> {
        serde_json::to_writer(&mut self.log_file, &op)?;
        self.log_file.write_all(b"\n")?;
        self.log_file.sync_all()?; // commit point
        let ret = self.state.apply(op);
        Ok(ret)
    }
}
