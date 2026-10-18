//! Broker state: applications, channels, functions, and the access-control and
//! schema-compatibility checks. This module is pure data + policy (no shared
//! memory I/O), so the security-relevant decisions are unit-testable.

use impulse_ring_core::avro::Fingerprint;
use impulse_ring_core::proto::{self, ChannelInfo, status};
use impulse_ring_core::util;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub struct ClientMeta {
  pub app_name: String,
  /// OS process id reported by the client at registration. `0` means unknown
  /// (an older connector that predates the `pid` field).
  pub pid: i32,
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

/// What to do with a channel whose publisher has just released it.
#[derive(Debug, PartialEq, Eq)]
pub enum Release {
  /// Reclaim the arena now; the name is already free. Somebody did subscribe,
  /// so nobody is still looking this channel up by name.
  Now(String),
  /// Keep it resolvable for now. Nobody ever subscribed, and a subscriber on
  /// its way (it learns the name from the response it is still reading) must
  /// be able to find the channel. [`Registry::reap_released`] takes it once
  /// the grace has passed.
  Deferred,
  /// Nothing released: the name is unknown, or it belongs to another client.
  Refused,
}

#[derive(Default)]
pub struct Registry {
  pub clients: HashMap<i64, ClientMeta>,
  channels: HashMap<i64, ChannelMeta>,
  chan_by_name: HashMap<String, i64>,
  functions: HashMap<i64, FunctionMeta>,
  fn_by_name: HashMap<String, i64>,
  /// Channels somebody has subscribed to at least once. The publisher's release
  /// of such a channel needs no grace period (see [`Release`]).
  subscribed: HashSet<i64>,
  /// Channels released by their publisher but kept resolvable until the grace
  /// passes, with the moment of release.
  released: HashMap<i64, Instant>,
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

  pub fn add_client(&mut self, app_name: String, pid: i32) -> i64 {
    let id = self.next_client;
    self.next_client += 1;
    self.clients.insert(id, ClientMeta { app_name, pid });
    id
  }

  /// If `name` is exposed by a function whose owner is *not* alive (per the
  /// `alive` predicate, called with the owner's stored pid), return that owner's
  /// client id so the caller can release it and free the name. Returns `None`
  /// when the name is free or its owner is still considered alive.
  ///
  /// The pid policy lives in the caller's predicate: the broker passes one that
  /// treats pid `0` (unknown / legacy connector) as alive, so a name is never
  /// reclaimed unless we positively know its owner has died.
  pub fn dead_function_owner(&self, name: &str, alive: impl Fn(i32) -> bool) -> Option<i64> {
    let fid = *self.fn_by_name.get(name)?;
    let owner = self.functions.get(&fid)?.owner;
    let pid = self.clients.get(&owner).map(|c| c.pid).unwrap_or(0);
    (!alive(pid)).then_some(owner)
  }

  /// Channel counterpart of [`dead_function_owner`].
  pub fn dead_channel_owner(&self, name: &str, alive: impl Fn(i32) -> bool) -> Option<i64> {
    let cid = *self.chan_by_name.get(name)?;
    let owner = self.channels.get(&cid)?.owner;
    let pid = self.clients.get(&owner).map(|c| c.pid).unwrap_or(0);
    (!alive(pid)).then_some(owner)
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
      if let Some(arena) = self.drop_channel(id) {
        arenas.push(arena);
      }
    }
    arenas
  }

  /// Forget channel `id` entirely — the entry, its name and its release
  /// bookkeeping — and hand back its arena name.
  fn drop_channel(&mut self, id: i64) -> Option<String> {
    let c = self.channels.remove(&id)?;
    self.chan_by_name.remove(&c.name);
    self.subscribed.remove(&id);
    self.released.remove(&id);
    Some(c.arena)
  }

  /// Note that `channel_id` now has a subscriber, so its publisher's release
  /// does not have to wait for one.
  pub fn mark_subscribed(&mut self, channel_id: i64) {
    self.subscribed.insert(channel_id);
  }

  /// A publisher is done with `name`: say whether the arena can go now — in
  /// which case the name is free again — or has to wait for a late subscriber
  /// (see [`Release`]).
  ///
  /// Only the channel's own owner may release it; anyone else is [`Release::Refused`].
  pub fn release_channel(&mut self, name: &str, owner: i64) -> Release {
    let Some(&id) = self.chan_by_name.get(name) else {
      return Release::Refused;
    };
    if self.channels.get(&id).map(|c| c.owner) != Some(owner) {
      return Release::Refused;
    }
    if self.subscribed.contains(&id) {
      return match self.drop_channel(id) {
        Some(arena) => Release::Now(arena),
        None => Release::Refused,
      };
    }
    self.released.insert(id, Instant::now());
    Release::Deferred
  }

  /// Reclaim every deferred release older than `grace`, returning their arena
  /// names. Called on the broker's idle tick.
  pub fn reap_released(&mut self, grace: Duration) -> Vec<String> {
    let due: Vec<i64> = self
      .released
      .iter()
      .filter(|(_, at)| at.elapsed() >= grace)
      .map(|(id, _)| *id)
      .collect();
    due.into_iter().filter_map(|id| self.drop_channel(id)).collect()
  }

  /// If `name` is held by a channel its publisher already released, take it
  /// back now and return its arena.
  ///
  /// A name inside its release grace must not refuse a fresh publish: the grace
  /// exists to let a late subscriber *find* the old channel, not to keep the
  /// name hostage after its publisher has gone.
  pub fn reap_if_released(&mut self, name: &str) -> Option<String> {
    let id = *self.chan_by_name.get(name)?;
    if !self.released.contains_key(&id) {
      return None;
    }
    self.drop_channel(id)
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
  fn dead_owner_detection() {
    let mut r = Registry::new();
    let owner = r.add_client("svc".into(), 4242);
    let fid = r.alloc_fn_id();
    r.insert_function(FunctionMeta {
      id: fid,
      name: "svc.http".into(),
      owner,
      req_fp: 1,
      resp_fp: 2,
      key: None,
      req_arena: "/impulse-ring.fn.1.v1".into(),
    });

    // Free name → never a dead owner.
    assert_eq!(r.dead_function_owner("absent", |_| false), None);
    // Owner reported alive → keep the name (refuse the new expose).
    assert_eq!(r.dead_function_owner("svc.http", |_| true), None);
    // Owner reported dead → hand back its id so the caller can reclaim.
    assert_eq!(r.dead_function_owner("svc.http", |_| false), Some(owner));
    // The predicate sees the stored pid.
    assert_eq!(r.dead_function_owner("svc.http", |pid| pid == 4242), None);
    assert_eq!(r.dead_function_owner("svc.http", |pid| pid != 4242), Some(owner));
  }

  #[test]
  fn unknown_pid_is_treated_per_predicate() {
    // A legacy client registers with pid 0. With the broker's real policy
    // (`pid == 0 || pid_alive(pid)`), pid 0 counts as alive → never reclaimed.
    let mut r = Registry::new();
    let owner = r.add_client("legacy".into(), 0);
    let cid = r.alloc_channel_id();
    r.insert_channel(ChannelMeta {
      id: cid,
      name: "legacy.events".into(),
      owner,
      schema_fp: 1,
      key: None,
      arena: "/impulse-ring.arena.1.v1".into(),
    });
    let broker_alive = |pid: i32| pid == 0 || pid == 12345 /* stand-in for pid_alive */;
    assert_eq!(r.dead_channel_owner("legacy.events", broker_alive), None);
  }

  /// A channel with a subscriber is reclaimed the moment its publisher lets go:
  /// nobody is left looking it up by name.
  #[test]
  fn a_subscribed_channel_is_released_at_once() {
    let mut r = Registry::new();
    let owner = r.add_client("svc".into(), 1);
    let cid = r.alloc_channel_id();
    r.insert_channel(ChannelMeta {
      id: cid,
      name: "body".into(),
      owner,
      schema_fp: 1,
      key: None,
      arena: "/impulse-ring.arena.1.v1".into(),
    });
    r.mark_subscribed(cid);

    assert_eq!(
      r.release_channel("body", owner),
      Release::Now("/impulse-ring.arena.1.v1".into())
    );
    // Gone for good: the name is free and there is nothing left to reap.
    assert!(!r.channel_name_taken("body"));
    assert!(r.reap_released(Duration::ZERO).is_empty());
  }

  /// A channel nobody ever subscribed to keeps its arena for the grace period.
  /// The subscriber learns the channel's name from a response it may still be
  /// reading, so releasing it at once would lose that race.
  #[test]
  fn a_released_channel_waits_for_a_late_subscriber() {
    let mut r = Registry::new();
    let owner = r.add_client("svc".into(), 1);
    let cid = r.alloc_channel_id();
    r.insert_channel(ChannelMeta {
      id: cid,
      name: "body".into(),
      owner,
      schema_fp: 1,
      key: None,
      arena: "/impulse-ring.arena.1.v1".into(),
    });

    assert_eq!(r.release_channel("body", owner), Release::Deferred);
    // Still resolvable, so a subscriber that arrives now still finds it.
    assert!(r.channel_name_taken("body"));
    assert!(r.resolve_subscribe(cid, "", 0).is_ok());
    // Nothing is due while the grace holds.
    assert!(r.reap_released(Duration::from_secs(3600)).is_empty());
    // Once it passes, the arena comes back.
    assert_eq!(r.reap_released(Duration::ZERO), vec!["/impulse-ring.arena.1.v1"]);
    assert!(!r.channel_name_taken("body"));
  }

  /// Releasing is not a way to take somebody else's channel away.
  #[test]
  fn only_the_owner_releases_a_channel() {
    let mut r = Registry::new();
    let owner = r.add_client("svc".into(), 1);
    let other = r.add_client("intruder".into(), 2);
    let cid = r.alloc_channel_id();
    r.insert_channel(ChannelMeta {
      id: cid,
      name: "body".into(),
      owner,
      schema_fp: 1,
      key: None,
      arena: "/impulse-ring.arena.1.v1".into(),
    });

    assert_eq!(r.release_channel("body", other), Release::Refused);
    assert_eq!(r.release_channel("absent", owner), Release::Refused);
    assert!(r.channel_name_taken("body"));
  }

  /// The grace is there to let a late subscriber find the old channel, not to
  /// hold the name after its publisher has gone.
  #[test]
  fn a_name_in_its_grace_does_not_refuse_a_fresh_publish() {
    let mut r = Registry::new();
    let owner = r.add_client("svc".into(), 1);
    let cid = r.alloc_channel_id();
    r.insert_channel(ChannelMeta {
      id: cid,
      name: "body".into(),
      owner,
      schema_fp: 1,
      key: None,
      arena: "/impulse-ring.arena.1.v1".into(),
    });
    assert_eq!(r.release_channel("body", owner), Release::Deferred);

    // The name is taken, but only by a release waiting out its grace.
    assert!(r.channel_name_taken("body"));
    assert_eq!(r.reap_if_released("body"), Some("/impulse-ring.arena.1.v1".into()));
    assert!(!r.channel_name_taken("body"));
    // And a live channel is never taken this way.
    let cid = r.alloc_channel_id();
    r.insert_channel(ChannelMeta {
      id: cid,
      name: "body".into(),
      owner,
      schema_fp: 1,
      key: None,
      arena: "/impulse-ring.arena.2.v1".into(),
    });
    assert_eq!(r.reap_if_released("body"), None);
  }

  /// A departing client takes its channels with it, release bookkeeping and all.
  #[test]
  fn a_departing_client_leaves_no_release_behind() {
    let mut r = Registry::new();
    let owner = r.add_client("svc".into(), 1);
    let cid = r.alloc_channel_id();
    r.insert_channel(ChannelMeta {
      id: cid,
      name: "body".into(),
      owner,
      schema_fp: 1,
      key: None,
      arena: "/impulse-ring.arena.1.v1".into(),
    });
    assert_eq!(r.release_channel("body", owner), Release::Deferred);

    assert_eq!(r.remove_client_channels(owner), vec!["/impulse-ring.arena.1.v1"]);
    // The arena must not be handed out a second time by the sweep.
    assert!(r.reap_released(Duration::ZERO).is_empty());
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
