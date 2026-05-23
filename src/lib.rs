use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut, Range},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub struct Sender<T> {
    send: usize,
    capacity: usize,
    shared: Arc<Shared<T>>,
}

pub struct Receiver<T> {
    recv: usize,
    capacity: usize,
    shared: Arc<Shared<T>>,
}

pub struct SendGuard<'a, T> {
    sender: &'a mut Sender<T>,
    range: Range<usize>,
}

pub struct RecvGuard<'a, T> {
    receiver: &'a mut Receiver<T>,
    range: Range<usize>,
}

struct Shared<T> {
    send: CacheAligned<AtomicUsize>,
    recv: CacheAligned<AtomicUsize>,
    data: Box<[UnsafeCell<T>]>,
}

unsafe impl<T> Send for Shared<T> where T: Send {}
unsafe impl<T> Sync for Shared<T> {}

#[derive(Debug)]
pub struct ChannelClosed;

#[repr(align(128))]
struct CacheAligned<T>(T);

pub fn channel<T>(capacity: usize, mut default: impl FnMut() -> T) -> (Sender<T>, Receiver<T>) {
    assert!(capacity.is_power_of_two());
    let mut data = Vec::with_capacity(capacity);
    for _ in 0..capacity {
        data.push(UnsafeCell::new(default()));
    }
    let data = data.into_boxed_slice();
    let shared = Arc::new(Shared {
        send: CacheAligned::new(AtomicUsize::new(0)),
        recv: CacheAligned::new(AtomicUsize::new(0)),
        data,
    });
    let sender = Sender {
        send: 0,
        capacity,
        shared: Arc::clone(&shared),
    };
    let receiver = Receiver {
        recv: 0,
        capacity,
        shared: Arc::clone(&shared),
    };
    (sender, receiver)
}

impl<T> Sender<T> {
    pub fn try_send(&mut self) -> Result<SendGuard<'_, T>, ChannelClosed> {
        if Arc::strong_count(&self.shared) == 1 {
            return Err(ChannelClosed);
        }
        let recv = self.shared.recv.load(Ordering::Acquire);
        let used = self.send.wrapping_sub(recv);
        let free = self.capacity - used;
        let start = self.send & (self.capacity - 1);
        let end = (start + free).min(self.capacity);
        let range = start..end;
        Ok(SendGuard {
            sender: self,
            range,
        })
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&mut self) -> Result<RecvGuard<'_, T>, ChannelClosed> {
        if Arc::strong_count(&self.shared) == 1 {
            return Err(ChannelClosed);
        }
        let send = self.shared.send.load(Ordering::Acquire);
        let used = send.wrapping_sub(self.recv);
        let start = self.recv & (self.capacity - 1);
        let end = (start + used).min(self.capacity);
        let range = start..end;
        Ok(RecvGuard {
            receiver: self,
            range,
        })
    }
}

impl<T> SendGuard<'_, T> {
    pub fn commit(self) {
        self.sender.send = self
            .sender
            .send
            .wrapping_add(self.range.end - self.range.start);
        self.sender
            .shared
            .send
            .store(self.sender.send, Ordering::Release);
    }
}

impl<T> RecvGuard<'_, T> {
    pub fn decommit(self) {
        self.receiver.recv = self
            .receiver
            .recv
            .wrapping_add(self.range.end - self.range.start);
        self.receiver
            .shared
            .recv
            .store(self.receiver.recv, Ordering::Release);
    }
}

impl<T> Deref for SendGuard<'_, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe {
            let pointer = self.sender.shared.data.as_ptr().add(self.range.start);
            let pointer = UnsafeCell::raw_get(pointer);
            std::slice::from_raw_parts(pointer, self.range.end - self.range.start)
        }
    }
}

impl<T> DerefMut for SendGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            let pointer = self.sender.shared.data.as_ptr().add(self.range.start);
            let pointer = UnsafeCell::raw_get(pointer);
            std::slice::from_raw_parts_mut(pointer, self.range.end - self.range.start)
        }
    }
}

impl<T> Deref for RecvGuard<'_, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe {
            let pointer = self.receiver.shared.data.as_ptr().add(self.range.start);
            let pointer = UnsafeCell::raw_get(pointer);
            std::slice::from_raw_parts(pointer, self.range.end - self.range.start)
        }
    }
}

impl<T> DerefMut for RecvGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            let pointer = self.receiver.shared.data.as_ptr().add(self.range.start);
            let pointer = UnsafeCell::raw_get(pointer);
            std::slice::from_raw_parts_mut(pointer, self.range.end - self.range.start)
        }
    }
}

impl<T> SendGuard<'_, T> {
    pub fn truncate(&mut self, new_length: usize) {
        let length = (self.range.end - self.range.start).min(new_length);
        self.range.end = self.range.start + length;
    }
}

impl<T> RecvGuard<'_, T> {
    pub fn truncate(&mut self, new_length: usize) {
        let length = (self.range.end - self.range.start).min(new_length);
        self.range.end = self.range.start + length;
    }
}

impl<T> CacheAligned<T> {
    pub fn new(value: T) -> Self { Self(value) }
}

impl<T> Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
