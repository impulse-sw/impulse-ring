//! Broker state: applications, channels, functions, and the access-control and
//! schema-compatibility checks. This module is pure data + policy (no shared
//! memory I/O), so the security-relevant decisions are unit-testable.

use impulse_ring_core::avro::Fingerprint;
use impulse_ring_core::proto::{self, ChannelInfo, status};
use impulse_ring_core::util;
use std::collections::HashMap;

pub struct ClientMeta {
  pub app_name: String,
}

pub struct ChannelMeta {
  pub id: i64,
  pub name: String,
  pub owner: i64,
  pub schema_fp: Fingerprint,
  pub key: Option<[u8; 32]>,
  pub arena: String,
}

pub struct FunctionMeta {
  pub id: i64,
  pub name: String,
  /// Owning client; used to reclaim the function when the client departs.
  pub owner: i64,
  pub req_fp: Fingerprint,
  pub resp_fp: Fingerprint,
  pub key: Option<[u8; 32]>,
  pub req_arena: String,
}

/// Outcome of an access/compatibility check: an OK or an error status + message.
pub type AccessResult = Result<(), (i32, String)>;

#[derive(Default)]
pub struct Registry {
  pub clients: HashMap<i64, ClientMeta>,
  channels: HashMap<i64, ChannelMeta>,
  chan_by_name: HashMap<String, i64>,
  functions: HashMap<i64, FunctionMeta>,
  fn_by_name: HashMap<String, i64>,
  next_client: i64,
  next_channel: i64,
  next_fn: i64,
}

impl Registry {
  pub fn new() -> Self {
    Registry {
      next_client: 1,
      next_channel: 1,
      next_fn: 1,
      ..Default::default()
    }
  }

  pub fn add_client(&mut self, app_name: String) -> i64 {
    let id = self.next_client;
    self.next_client += 1;
    self.clients.insert(id, ClientMeta { app_name });
    id
  }

  pub fn remove_client(&mut self, id: i64) {
    self.clients.remove(&id);
  }

  /// Remove every channel owned by `owner`, returning their arena names so the
  /// caller can unlink the backing shared memory.
  pub fn remove_client_channels(&mut self, owner: i64) -> Vec<String> {
    let ids: Vec<i64> = self
      .channels
      .iter()
      .filter(|(_, c)| c.owner == owner)
      .map(|(id, _)| *id)
      .collect();
    let mut arenas = Vec::new();
    for id in ids {
      if let Some(c) = self.channels.remove(&id) {
        self.chan_by_name.remove(&c.name);
        arenas.push(c.arena);
      }
    }
    arenas
  }

  /// Remove every function owned by `owner`, returning their request-arena names.
  pub fn remove_client_functions(&mut self, owner: i64) -> Vec<String> {
    let ids: Vec<i64> = self
      .functions
      .iter()
      .filter(|(_, f)| f.owner == owner)
      .map(|(id, _)| *id)
      .collect();
    let mut arenas = Vec::new();
    for id in ids {
      if let Some(f) = self.functions.remove(&id) {
        self.fn_by_name.remove(&f.name);
        arenas.push(f.req_arena);
      }
    }
    arenas
  }

  pub fn alloc_channel_id(&mut self) -> i64 {
    let id = self.next_channel;
    self.next_channel += 1;
    id
  }

  pub fn alloc_fn_id(&mut self) -> i64 {
    let id = self.next_fn;
    self.next_fn += 1;
    id
  }

  pub fn channel_name_taken(&self, name: &str) -> bool {
    self.chan_by_name.contains_key(name)
  }

  pub fn insert_channel(&mut self, c: ChannelMeta) {
    self.chan_by_name.insert(c.name.clone(), c.id);
    self.channels.insert(c.id, c);
  }

  pub fn function_name_taken(&self, name: &str) -> bool {
    self.fn_by_name.contains_key(name)
  }

  pub fn insert_function(&mut self, f: FunctionMeta) {
    self.fn_by_name.insert(f.name.clone(), f.id);
    self.functions.insert(f.id, f);
  }

  pub fn find_function_by_name(&self, name: &str) -> Option<&FunctionMeta> {
    self.fn_by_name.get(name).and_then(|id| self.functions.get(id))
  }

  pub fn list_channels(&self) -> Vec<ChannelInfo> {
    self
      .channels
      .values()
      .map(|c| ChannelInfo {
        channel_id: c.id,
        name: c.name.clone(),
        owner_app: self
          .clients
          .get(&c.owner)
          .map(|m| m.app_name.clone())
          .unwrap_or_default(),
        schema_fp: proto::fp_to_i64(c.schema_fp),
        requires_key: c.key.is_some(),
      })
      .collect()
  }

  /// Resolve a subscribe request: enforce key ACL and schema fingerprint
  /// compatibility, returning the channel's arena + fingerprint on success.
  pub fn resolve_subscribe(
    &self,
    channel_id: i64,
    access_key: &str,
    expected_fp: Fingerprint,
  ) -> Result<(String, Fingerprint), (i32, String)> {
    let c = self
      .channels
      .get(&channel_id)
      .ok_or((status::ERR_NOT_FOUND, "no such channel".into()))?;
    check_key(&c.key, access_key)?;
    check_fp(expected_fp, c.schema_fp)?;
    Ok((c.arena.clone(), c.schema_fp))
  }

  /// Resolve a function lookup: enforce key ACL, returning function metadata.
  pub fn resolve_lookup(
    &self,
    name: &str,
    access_key: &str,
  ) -> Result<(i64, Fingerprint, Fingerprint, String), (i32, String)> {
    let f = self
      .find_function_by_name(name)
      .ok_or((status::ERR_NOT_FOUND, "no such function".into()))?;
    check_key(&f.key, access_key)?;
    Ok((f.id, f.req_fp, f.resp_fp, f.req_arena.clone()))
  }
}

/// Enforce a channel/function key: a `None` policy is public; a `Some(hash)`
/// policy requires the presented key to match.
pub fn check_key(policy: &Option<[u8; 32]>, presented: &str) -> AccessResult {
  match policy {
    None => Ok(()),
    Some(hash) => {
      if util::key_matches(presented, hash) {
        Ok(())
      } else {
        Err((status::ERR_DENIED, "invalid access key".into()))
      }
    }
  }
}

/// Enforce schema fingerprint compatibility. `expected == 0` means "no
/// expectation" and always passes.
pub fn check_fp(expected: Fingerprint, actual: Fingerprint) -> AccessResult {
  if expected == 0 || expected == actual {
    Ok(())
  } else {
    Err((
      status::ERR_SCHEMA_MISMATCH,
      format!("schema mismatch: expected {expected:#x}, got {actual:#x}"),
    ))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn key_acl() {
    let h = util::key_hash("secret");
    assert!(check_key(&None, "").is_ok());
    assert!(check_key(&None, "whatever").is_ok());
    assert!(check_key(&Some(h), "secret").is_ok());
    assert_eq!(check_key(&Some(h), "wrong").unwrap_err().0, status::ERR_DENIED);
  }

  #[test]
  fn fp_compat() {
    assert!(check_fp(0, 123).is_ok()); // no expectation
    assert!(check_fp(123, 123).is_ok());
    assert_eq!(check_fp(123, 456).unwrap_err().0, status::ERR_SCHEMA_MISMATCH);
  }

  #[test]
  fn subscribe_resolution() {
    let mut r = Registry::new();
    let cid = r.alloc_channel_id();
    r.insert_channel(ChannelMeta {
      id: cid,
      name: "metrics".into(),
      owner: 1,
      schema_fp: 0xABCD,
      key: Some(util::key_hash("k")),
      arena: "/impulse-ring.arena.1.v1".into(),
    });
    // wrong key denied
    assert_eq!(r.resolve_subscribe(cid, "bad", 0).unwrap_err().0, status::ERR_DENIED);
    // right key, wrong fp
    assert_eq!(
      r.resolve_subscribe(cid, "k", 0x1111).unwrap_err().0,
      status::ERR_SCHEMA_MISMATCH
    );
    // right key, matching fp
    let (arena, fp) = r.resolve_subscribe(cid, "k", 0xABCD).unwrap();
    assert_eq!(fp, 0xABCD);
    assert!(arena.ends_with(".v1"));
    // unknown channel
    assert_eq!(r.resolve_subscribe(999, "k", 0).unwrap_err().0, status::ERR_NOT_FOUND);
  }
}
