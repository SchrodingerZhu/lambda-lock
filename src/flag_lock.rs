use core::sync::atomic::{AtomicBool, Ordering};

#[repr(transparent)]
pub(crate) struct FlagLock(AtomicBool);

impl FlagLock {
    pub(crate) const fn new() -> Self {
        Self(AtomicBool::new(false))
    }
    #[inline(always)]
    pub(crate) fn try_acquire(&self) -> bool {
        !self.0.swap(true, Ordering::Acquire)
    }
    #[inline(always)]
    pub(crate) fn acquire(&self) {
        while self.0.swap(true, Ordering::Acquire) {
            while self.0.load(Ordering::Relaxed) {
                core::hint::spin_loop();
            }
        }
    }
    #[inline(always)]
    pub(crate) unsafe fn release(&self) {
        self.0.store(false, Ordering::Release);
    }
}
