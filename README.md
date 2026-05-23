# buffer-channel: transactional batch FIFO

## Usage 

```rs
let (sender, receiver) = buffer_channel::channel(1024, || 0u8);

// Acquire a send guard.
let mut guard = sender.try_send().expect("channel was dropped");

// Write a message into the buffer.
let message = b"hello!";
guard[0..message.len()].copy_from_slice(message);

// Truncate the send buffer to the message length and commit the message.
guard.truncate(message.len());
guard.commit()

// Acquire a write guard
let guard = receiver.try_recv().expect("channel was dropped");
println!("recv: {}", std::str::from_utf8(&guard).unwrap());
guard.commit()
```