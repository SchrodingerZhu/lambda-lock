#![no_std]
mod flag_lock;
mod futex;

use core::{
    cell::{Cell, UnsafeCell},
    mem::MaybeUninit,
    ptr::NonNull,
    sync::atomic::{AtomicBool, AtomicPtr, Ordering},
};

use flag_lock::{FlagLock, FlagLockOwnership};
use futex::Futex;

const WAITING: u32 = 0;
const DONE: u32 = 1;
const READY: u32 = 2;
const SLEEPING: u32 = 3;
const POISONED: u32 = 4;

#[derive(Clone, Copy)]
pub enum LockError<'a, T> {
    Poisoned(&'a UnsafeCell<T>),
}

impl<'a, T> core::fmt::Display for LockError<'a, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "lock is poisoned")
    }
}

impl<'a, T> core::fmt::Debug for LockError<'a, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "LockError")
    }
}

impl<'a, T> core::error::Error for LockError<'a, T> {}

impl<'a, T> LockError<'a, T> {
    pub fn get_ref(&self) -> &'a T {
        match self {
            LockError::Poisoned(data) => unsafe { &*data.get() },
        }
    }
    pub fn get_mut(&mut self) -> &'a mut T {
        match self {
            LockError::Poisoned(data) => unsafe { &mut *data.get() },
        }
    }
}

struct LockNode {
    status: Futex,
    next: AtomicPtr<Self>,
    lambda: fn(NonNull<Self>),
}

struct RawLambdaLock {
    head: AtomicPtr<LockNode>,
    flag_lock: FlagLock,
    is_poisoned: AtomicBool,
}

struct ExecGuard<'a> {
    lock: &'a RawLambdaLock,
    current: NonNull<LockNode>,
}

impl Drop for ExecGuard<'_> {
    #[cold]
    fn drop(&mut self) {
        self.lock.is_poisoned.store(true, Ordering::Release);
        unsafe {
            loop {
                let next = self.current.as_ref().next.load(Ordering::Acquire);
                if let Some(next) = NonNull::new(next) {
                    self.current.as_ref().status.notify(POISONED, SLEEPING);
                    self.current = next;
                    continue;
                }
                if self
                    .lock
                    .head
                    .compare_exchange(
                        self.current.as_ptr(),
                        core::ptr::null_mut(),
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.current.as_ref().status.notify(POISONED, SLEEPING);
                    return;
                }
                // continue to notify the next node. this will not last long since the poisoned flag is set.
                let mut next = Option::None;
                while next.is_none() {
                    core::hint::spin_loop();
                    next = NonNull::new(self.current.as_ref().next.load(Ordering::Acquire));
                }
                self.current.as_ref().status.notify(POISONED, SLEEPING);
                self.current = next.unwrap_unchecked();
            }
        }
    }
}

impl LockNode {
    fn new(lambda: fn(NonNull<Self>)) -> Self {
        Self {
            status: Futex::new(WAITING),
            next: AtomicPtr::new(core::ptr::null_mut()),
            lambda,
        }
    }

    #[inline(always)]
    unsafe fn execute(this: NonNull<Self>) {
        (this.as_ref().lambda)(this);
    }

    #[inline(always)]
    unsafe fn enqueue(this: NonNull<Self>, lock: &RawLambdaLock) -> bool {
        if lock.head.load(Ordering::Relaxed).is_null() && lock.flag_lock.try_acquire() {
            let _ownership = FlagLockOwnership::annouce(&lock.flag_lock);
            if lock.is_poisoned.load(Ordering::Acquire) {
                return false;
            }
            struct LocalExecGuard<'a>(&'a RawLambdaLock);
            impl Drop for LocalExecGuard<'_> {
                fn drop(&mut self) {
                    self.0.is_poisoned.store(true, Ordering::Release);
                }
            }
            let guard = LocalExecGuard(lock);
            Self::execute(this);
            core::mem::forget(guard);
            return true;
        }
        Self::enqueue_slow(this, lock)
    }

    #[cold]
    unsafe fn enqueue_slow(this: NonNull<Self>, lock: &RawLambdaLock) -> bool {
        if lock.is_poisoned.load(Ordering::Acquire) {
            return false;
        }
        let prev = lock.head.swap(this.as_ptr(), Ordering::AcqRel);

        if let Some(prev) = NonNull::new(prev) {
            prev.as_ref().next.store(this.as_ptr(), Ordering::Release);
            let mut i = 100;
            while i > 0 {
                if this.as_ref().status.load(Ordering::Acquire) != WAITING {
                    break;
                }
                core::hint::spin_loop();
                i -= 1;
            }
            if i == 0
                && this
                    .as_ref()
                    .status
                    .compare_exchange(WAITING, SLEEPING, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                this.as_ref().status.wait(SLEEPING);
            }
            let val = this.as_ref().status.load(Ordering::Acquire);
            if val == DONE {
                return true;
            }
            if val == POISONED {
                return false;
            }
        } else {
            lock.flag_lock.acquire();
        }

        let ownership = FlagLockOwnership::annouce(&lock.flag_lock);

        let mut current = this;

        loop {
            let guard = ExecGuard { lock, current };
            Self::execute(current);
            core::mem::forget(guard);
            let next = current.as_ref().next.load(Ordering::Acquire);
            if let Some(next) = NonNull::new(next) {
                current.as_ref().status.notify(DONE, SLEEPING);
                current = next;
                continue;
            }
            break;
        }

        if lock
            .head
            .compare_exchange(
                current.as_ptr(),
                core::ptr::null_mut(),
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            current.as_ref().status.notify(DONE, SLEEPING);
            return true;
        }

        ownership.handover();

        while current.as_ref().next.load(Ordering::Relaxed).is_null() {
            core::hint::spin_loop();
        }

        let next = NonNull::new_unchecked(current.as_ref().next.load(Ordering::Acquire));
        next.as_ref().status.notify(READY, SLEEPING);
        current.as_ref().status.notify(DONE, SLEEPING);

        true
    }
}

pub struct LambdaLock<T> {
    inner: RawLambdaLock,
    data: UnsafeCell<T>,
}

unsafe impl<T> Sync for LambdaLock<T> {}

impl<T> LambdaLock<T> {
    pub fn new(data: T) -> Self {
        let inner = RawLambdaLock {
            head: AtomicPtr::new(core::ptr::null_mut()),
            flag_lock: FlagLock::new(),
            is_poisoned: AtomicBool::new(false),
        };
        let data = UnsafeCell::new(data);
        Self { inner, data }
    }
    pub fn clear_poisoned(&self) {
        self.inner.is_poisoned.store(false, Ordering::Release);
    }
    pub fn schedule<F, R>(&self, lambda: F) -> core::result::Result<R, LockError<T>>
    where
        F: FnOnce(&mut T) -> R + Send,
        R: Send,
    {
        #[repr(C)]
        struct Node<T, F, R> {
            inner: UnsafeCell<LockNode>,
            ret: Cell<MaybeUninit<R>>,
            lambda: MaybeUninit<F>,
            data: NonNull<T>,
        }
        fn execute<T, F, R>(this: NonNull<LockNode>)
        where
            F: FnOnce(&mut T) -> R + Send,
            R: Send,
        {
            unsafe {
                let this = NonNull::cast::<Node<T, F, R>>(this);
                let lambda = this.as_ref().lambda.assume_init_read();
                let mut data = this.as_ref().data;
                let ret = (lambda)(data.as_mut());
                this.as_ref().ret.set(MaybeUninit::new(ret));
            }
        }
        unsafe {
            let node = Node {
                inner: UnsafeCell::new(LockNode::new(execute::<T, F, R>)),
                ret: Cell::new(MaybeUninit::uninit()),
                lambda: MaybeUninit::new(lambda),
                data: NonNull::new_unchecked(self.data.get()),
            };

            let inner = NonNull::from(&node).cast();
            if LockNode::enqueue(inner, &self.inner) {
                Ok(node.ret.into_inner().assume_init())
            } else {
                Err(LockError::Poisoned(&self.data))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;

    #[test]
    fn smoke_test() {
        let lock = LambdaLock::new(0);
        lock.schedule(|data| {
            *data += 1;
        })
        .unwrap();
        assert_eq!(lock.schedule(|x| *x).unwrap(), 1);
    }

    #[test]
    fn multi_thread_test() {
        let cnt = if cfg!(miri) { 8 } else { 100 };
        let lock = LambdaLock::new(0);
        std::thread::scope(|scope| {
            for i in 0..cnt {
                let lock = &lock;
                scope.spawn(move || {
                    lock.schedule(|data| {
                        *data += cnt - i;
                    })
                    .unwrap();
                });
            }
        });

        assert_eq!(lock.schedule(|x| *x).unwrap(), cnt * (cnt + 1) / 2);
    }

    #[test]
    #[should_panic]
    fn mutli_thread_panic_chain_test() {
        let cnt = if cfg!(miri) { 8 } else { 100 };
        let lock = LambdaLock::new(0);
        std::thread::scope(|scope| {
            for i in 0..cnt {
                let lock = &lock;
                scope.spawn(move || {
                    lock.schedule(|data| {
                        *data += cnt - i;
                        if i == cnt / 2 {
                            panic!("panic chain");
                        }
                    })
                    .unwrap();
                });
            }
        });
    }

    #[test]
    fn mutli_thread_panic_unpoison_test() {
        let cnt = if cfg!(miri) { 8 } else { 100 };
        let lock = LambdaLock::new(0);
        std::thread::scope(|scope| {
            let mut handle = None;
            for i in 0..cnt {
                let lock = &lock;
                let panic_handle = scope.spawn(move || {
                    lock.schedule(|data| {
                        if i == cnt / 2 {
                            panic!("panic chain");
                        }
                        *data += cnt - i;
                    })
                    .unwrap_or_else(|_| {
                        lock.clear_poisoned();
                        // reschedule the task
                        lock.schedule(|data| {
                            *data += cnt - i;
                        })
                        .unwrap();
                    });
                });
                if i == cnt / 2 {
                    handle = Some(panic_handle);
                }
            }
            handle.unwrap().join().unwrap_err();
        });
        assert_eq!(
            lock.schedule(|x| *x).unwrap_or_else(|x| *x.get_ref()),
            cnt * cnt / 2
        );
    }
}
