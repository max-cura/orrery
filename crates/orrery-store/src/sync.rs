use std::sync::atomic::{AtomicU32, Ordering};

#[repr(transparent)]
pub struct VersionLock(AtomicU32);

const LOCK_VERSION_BIT : u32 = 1;
const INCR_VERSION_BIT : u32 = 1 << 8;
const VERSION_BIT_MASK : u32 = 0xffff_ff00;

pub struct Version(u32);

impl Version {
    pub fn is_write_locked(&self) -> bool {
        (self.0 & INCR_VERSION_BIT) != 0
    }
    pub fn version_no(&self) -> u32 {
        self.0 & VERSION_BIT_MASK
    }
}

pub struct WriteGuard<'a> {
    inner: &'a VersionLock,
}

impl<'a> WriteGuard<'a> {
    pub fn new(inner: &'a VersionLock) -> Self {
        Self { inner }
    }
    pub fn set_dirty(&mut self) {
        if !self.inner.version().is_write_locked() {
            unsafe { self.inner.incr_version() }
        }
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        if self.inner.version().is_write_locked() {
            unsafe { self.inner.incr_version() }
        }
        unsafe { self.inner.write_lock_release() }
    }
}

impl VersionLock {
    pub fn version(&self) -> Version {
        Version(self.0.load(Ordering::Acquire))
    }
    unsafe fn write_lock_release(&self) {
        self.0.fetch_and(!LOCK_VERSION_BIT, Ordering::AcqRel);
    }
    pub fn try_write_lock_versioned(&self, version: Version) -> Result<WriteGuard, ()> {
        let x = self.0.fetch_or(LOCK_VERSION_BIT, Ordering::AcqRel);
        if x & LOCK_VERSION_BIT != 0 {
            return Err(())
        } else {
            if (x & VERSION_BIT_MASK) != version.version_no() {
                unsafe { self.write_lock_release() }
                return Err(())
            }
        }
        Ok(WriteGuard::new(self))
    }
    unsafe fn incr_version(&self) {
        self.0.fetch_add(INCR_VERSION_BIT, Ordering::AcqRel);
    }
}
