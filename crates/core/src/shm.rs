//! POSIX shared-memory segments (`shm_open` + `mmap`).
//!
//! We use *named* segments rather than `memfd_create` on purpose: passing a
//! `memfd` to another process requires `SCM_RIGHTS` over a Unix socket, and
//! sockets are explicitly out of scope. Named segments give us socket-free
//! discovery — a peer maps a segment purely from its well-known name.
//!
//! Names follow the POSIX convention and must start with `/`; on Linux they
//! surface as files under `/dev/shm`.

use std::ffi::CString;
use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64};

/// Directory where Linux exposes POSIX shared memory.
pub const SHM_DIR: &str = "/dev/shm";
/// Common prefix for every segment owned by Ring, used for crash cleanup scans.
pub const NAME_PREFIX: &str = "impulse-ring.";
/// Permission bits for Ring segments: world read/write.
///
/// Ring is socket-free, single-machine IPC: peers rendezvous purely through
/// `/dev/shm` names. Those peers routinely run as *different* users (e.g. an
/// LBRP front under root talking to an `impulsed` broker under a service user),
/// and a peer that only opens a foreign segment still needs `O_RDWR` on it to
/// push replies. Owner-only bits (`0o600`) make any cross-UID open fail with
/// `EACCES`, so we deliberately open the segments to everyone on the host.
const SEGMENT_MODE: libc::mode_t = 0o666;

/// An mmap'd POSIX shared-memory segment.
pub struct Segment {
  name: String,
  ptr: *mut u8,
  len: usize,
  /// Only the creator unlinks the name on drop; openers just unmap.
  owns_unlink: bool,
}

// The mapping is shared across threads/processes; access is mediated by the
// atomic ring/header fields placed inside it, so the raw pointer is Send/Sync.
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

impl Segment {
  /// Create (or truncate) a segment of exactly `size` bytes and map it RW.
  pub fn create(name: &str, size: usize) -> io::Result<Segment> {
    debug_assert!(name.starts_with('/'), "shm name must start with '/'");
    let cname = CString::new(name).map_err(|_| io::Error::other("nul in shm name"))?;
    let fd = unsafe {
      libc::shm_open(
        cname.as_ptr(),
        libc::O_CREAT | libc::O_RDWR,
        SEGMENT_MODE as libc::c_uint,
      )
    };
    if fd < 0 {
      return Err(io::Error::last_os_error());
    }
    let res = (|| {
      // `shm_open` masks the requested mode by the process umask, which would
      // typically strip the group/other write bits we need for cross-UID peers.
      // `fchmod` sets the final bits unconditionally.
      if unsafe { libc::fchmod(fd, SEGMENT_MODE) } < 0 {
        return Err(io::Error::last_os_error());
      }
      if unsafe { libc::ftruncate(fd, size as libc::off_t) } < 0 {
        return Err(io::Error::last_os_error());
      }
      let ptr = unsafe {
        libc::mmap(
          std::ptr::null_mut(),
          size,
          libc::PROT_READ | libc::PROT_WRITE,
          libc::MAP_SHARED,
          fd,
          0,
        )
      };
      if ptr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
      }
      Ok(ptr as *mut u8)
    })();
    unsafe { libc::close(fd) };
    let ptr = res?;
    Ok(Segment {
      name: name.to_string(),
      ptr,
      len: size,
      owns_unlink: true,
    })
  }

  /// Open an existing segment created by another process and map it RW.
  pub fn open(name: &str) -> io::Result<Segment> {
    let cname = CString::new(name).map_err(|_| io::Error::other("nul in shm name"))?;
    let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600 as libc::c_uint) };
    if fd < 0 {
      return Err(io::Error::last_os_error());
    }
    let res = (|| {
      let mut st: libc::stat = unsafe { std::mem::zeroed() };
      if unsafe { libc::fstat(fd, &mut st) } < 0 {
        return Err(io::Error::last_os_error());
      }
      let size = st.st_size as usize;
      if size == 0 {
        return Err(io::Error::other("segment has zero size"));
      }
      let ptr = unsafe {
        libc::mmap(
          std::ptr::null_mut(),
          size,
          libc::PROT_READ | libc::PROT_WRITE,
          libc::MAP_SHARED,
          fd,
          0,
        )
      };
      if ptr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
      }
      Ok((ptr as *mut u8, size))
    })();
    unsafe { libc::close(fd) };
    let (ptr, len) = res?;
    Ok(Segment {
      name: name.to_string(),
      ptr,
      len,
      owns_unlink: false,
    })
  }

  pub fn name(&self) -> &str {
    &self.name
  }

  pub fn len(&self) -> usize {
    self.len
  }

  pub fn is_empty(&self) -> bool {
    self.len == 0
  }

  pub fn as_ptr(&self) -> *const u8 {
    self.ptr
  }

  pub fn as_mut_ptr(&self) -> *mut u8 {
    self.ptr
  }

  /// Reinterpret the bytes at `offset` as `&T`. Caller guarantees layout/init.
  ///
  /// # Safety
  /// `offset + size_of::<T>()` must be within the mapping and the bytes must
  /// be a valid, properly aligned `T` (typically an atomic header struct).
  pub unsafe fn atomic_u32_at(&self, offset: usize) -> &AtomicU32 {
    debug_assert!(offset + 4 <= self.len);
    unsafe { &*(self.ptr.add(offset) as *const AtomicU32) }
  }

  /// # Safety
  /// Same contract as [`atomic_u32_at`](Self::atomic_u32_at) for `AtomicU64`.
  pub unsafe fn atomic_u64_at(&self, offset: usize) -> &AtomicU64 {
    debug_assert!(offset + 8 <= self.len);
    unsafe { &*(self.ptr.add(offset) as *const AtomicU64) }
  }
}

impl Drop for Segment {
  fn drop(&mut self) {
    unsafe {
      libc::munmap(self.ptr as *mut libc::c_void, self.len);
      if self.owns_unlink {
        let _ = unlink(&self.name);
      }
    }
  }
}

/// Remove a segment's name from the system (idempotent; ENOENT is ignored).
pub fn unlink(name: &str) -> io::Result<()> {
  let cname = CString::new(name).map_err(|_| io::Error::other("nul in shm name"))?;
  let r = unsafe { libc::shm_unlink(cname.as_ptr()) };
  if r < 0 {
    let e = io::Error::last_os_error();
    if e.raw_os_error() == Some(libc::ENOENT) {
      return Ok(());
    }
    return Err(e);
  }
  Ok(())
}

/// List existing Ring segment names (as POSIX shm names, with leading `/`).
///
/// Used by the broker at startup to garbage-collect segments left behind by a
/// previous crashed run.
pub fn list_segments() -> io::Result<Vec<String>> {
  let mut out = Vec::new();
  for entry in std::fs::read_dir(SHM_DIR)? {
    let entry = entry?;
    let fname = entry.file_name();
    let fname = fname.to_string_lossy();
    if fname.starts_with(NAME_PREFIX) {
      out.push(format!("/{fname}"));
    }
  }
  Ok(out)
}

/// True if a process with `pid` is alive (`kill(pid, 0)` probe).
pub fn pid_alive(pid: i32) -> bool {
  if pid <= 0 {
    return false;
  }
  let r = unsafe { libc::kill(pid as libc::pid_t, 0) };
  if r == 0 {
    return true;
  }
  // EPERM means it exists but we can't signal it -> still alive.
  io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}
