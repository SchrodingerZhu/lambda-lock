#![no_std]
mod flag_lock;
mod futex;

use core::{
    cell::{Cell, UnsafeCell},
    mem::MaybeUninit,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, Ordering},
};


use flag_lock::{FlagLock, FlagLockOwnership};
use futex::Futex;

const WAITING: u32 = 0b00;
const DONE: u32 = 0b01;
const READY: u32 = 0b10;
const SLEEPING: u32 = 0b11;

struct LockNode {
    status: Futex,
    next: AtomicPtr<Self>,
    lambda: fn(NonNull<Self>),
}

struct RawLambdaLock {
    head: AtomicPtr<LockNode>,
    flag_lock: FlagLock,
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
    unsafe fn enqueue(this: NonNull<Self>, lock: &RawLambdaLock) {
        if lock.head.load(Ordering::Relaxed).is_null() && lock.flag_lock.try_acquire() {
            let _ownership = FlagLockOwnership::annouce(&lock.flag_lock);
            Self::execute(this);
            return;
        }
        Self::enqueue_slow(this, lock);
    }

    #[cold]
    unsafe fn enqueue_slow(this: NonNull<Self>, lock: &RawLambdaLock) {
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
            if this.as_ref().status.load(Ordering::Acquire) == DONE {
                return;
            }
        } else {
            lock.flag_lock.acquire();
        }

        let ownership = FlagLockOwnership::annouce(&lock.flag_lock);

        let mut current = this;
        loop {
            Self::execute(current);
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
            return;
        }

        ownership.handover();

        while current.as_ref().next.load(Ordering::Relaxed).is_null() {
            core::hint::spin_loop();
        }

        let next = NonNull::new_unchecked(current.as_ref().next.load(Ordering::Acquire));
        next.as_ref().status.notify(READY, SLEEPING);
        current.as_ref().status.notify(DONE, SLEEPING);
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
        };
        let data = UnsafeCell::new(data);
        Self { inner, data }
    }
    pub fn schedule<F, R>(&self, lambda: F) -> R
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
            LockNode::enqueue(inner, &self.inner);
            node.ret.into_inner().assume_init()
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;

    #[test]
    fn test() {
        let lock = LambdaLock::new(0);
        lock.schedule(|data| {
            *data += 1;
        });
        assert_eq!(lock.schedule(|x| *x), 1);
    }

    #[test]
    fn test2() {
        let cnt = if cfg!(miri) { 8 } else { 100 };
        let lock = LambdaLock::new(0);
        std::thread::scope(|scope| {
            for i in 0..cnt {
                let lock = &lock;
                scope.spawn(move || {
                    lock.schedule(|data| {
                        *data += cnt - i;
                    });
                });
            }
        });

        assert_eq!(lock.schedule(|x| *x), cnt * (cnt + 1) / 2);
    }
}
