use core::sync::atomic::{AtomicU32, Ordering};

use rustix::{
    io::Errno,
    thread::{FutexFlags, FutexOperation},
};

#[repr(transparent)]
pub(crate) struct Futex(AtomicU32);

impl core::ops::Deref for Futex {
    type Target = AtomicU32;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Futex {
    #[inline(always)]
    pub(crate) const fn new(value: u32) -> Self {
        Self(AtomicU32::new(value))
    }

    #[inline(always)]
    pub(crate) fn wait(&self, value: u32) {
        #[cfg(not(miri))]
        unsafe {
            while self.load(Ordering::Acquire) == value {
                while let Err(Errno::INTR) = rustix::thread::futex(
                    &self as *const _ as *mut _,
                    FutexOperation::Wait,
                    FutexFlags::PRIVATE,
                    value,
                    core::ptr::null(),
                    core::ptr::null_mut(),
                    0,
                ) {
                    core::hint::spin_loop();
                }
            }
        }
        #[cfg(miri)]
        {
            while self.load(Ordering::Acquire) == value {
                core::hint::spin_loop();
            }
        }
    }

    #[inline(always)]
    pub(crate) fn notify(&self, new_val: u32, #[allow(unused)] old_val: u32) {
        #[cfg(not(miri))]
        unsafe {
            if self.swap(new_val, Ordering::AcqRel) == old_val {
                let _ = rustix::thread::futex(
                    &self as *const _ as *mut _,
                    FutexOperation::Wake,
                    FutexFlags::PRIVATE,
                    1,
                    core::ptr::null(),
                    core::ptr::null_mut(),
                    0,
                );
            }
        }
        #[cfg(miri)]
        {
            self.store(new_val, Ordering::Release);
        }
    }
}
