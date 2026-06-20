//! `impulsed` — the Ring broker daemon.
//!
//! Pure shared-memory IPC broker: no HTTP, no sockets. It owns the well-known
//! control segment under `/dev/shm` and the data arenas it hands out to
//! connectors. Linux-only (Tier 0: arm64/amd64).

#![deny(warnings, clippy::todo, clippy::unimplemented)]

use impulsed::broker::{self, StartError};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn main() {
  // Minimal stderr logger (no external logger dep for the daemon).
  init_logger();

  let mut broker = match broker::Broker::start() {
    Ok(b) => b,
    Err(StartError::AlreadyRunning) => {
      // Another broker already owns the bus; nothing to do. Exit cleanly so
      // supervisors and CI steps don't treat this as a failure.
      eprintln!("impulsed: another broker is already running; nothing to do");
      return;
    }
    Err(StartError::Io(e)) => {
      eprintln!("impulsed: failed to start: {e}");
      std::process::exit(1);
    }
  };

  let running = Arc::new(AtomicBool::new(true));
  install_signal_handler(running.clone());

  let r = running.clone();
  broker.run(move || r.load(Ordering::Relaxed));
  eprintln!("impulsed: shutting down");
}

fn init_logger() {
  use std::io::Write;
  struct Stderr;
  impl log::Log for Stderr {
    fn enabled(&self, _: &log::Metadata) -> bool {
      true
    }
    fn log(&self, record: &log::Record) {
      let _ = writeln!(std::io::stderr(), "[{}] {}", record.level(), record.args());
    }
    fn flush(&self) {}
  }
  let _ = log::set_boxed_logger(Box::new(Stderr));
  log::set_max_level(log::LevelFilter::Info);
}

/// Install SIGINT/SIGTERM handlers that flip the run flag so `Drop` can unlink
/// segments cleanly.
fn install_signal_handler(running: Arc<AtomicBool>) {
  static FLAG: AtomicBool = AtomicBool::new(false);
  extern "C" fn handler(_: libc::c_int) {
    FLAG.store(true, Ordering::SeqCst);
  }
  let h = handler as *const () as libc::sighandler_t;
  unsafe {
    libc::signal(libc::SIGINT, h);
    libc::signal(libc::SIGTERM, h);
  }
  // Bridge the static flag to the broker's run flag via a watcher thread.
  std::thread::spawn(move || {
    loop {
      if FLAG.load(Ordering::SeqCst) {
        running.store(false, Ordering::Relaxed);
        break;
      }
      std::thread::sleep(std::time::Duration::from_millis(50));
    }
  });
}
