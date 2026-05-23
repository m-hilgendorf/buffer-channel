use buffer_channel::channel;
use cpal::{
    BufferSize, InputCallbackInfo, OutputCallbackInfo, StreamConfig, StreamError,
    traits::{DeviceTrait, HostTrait},
};

// Demo showing the synchronization of two i/o device callbacks with different buffer sizes through one channel.
fn main() {
    let host = cpal::default_host();

    // Input device and config.
    let input_device = host.default_input_device().unwrap();
    let input_config = input_device.default_input_config().unwrap().into();
    let input_config = StreamConfig {
        buffer_size: BufferSize::Fixed(513),
        ..input_config
    };

    // Output device and config.
    let output_device = host.default_output_device().unwrap();
    let output_config = output_device.default_output_config().unwrap().into();
    let output_config = StreamConfig {
        buffer_size: BufferSize::Fixed(499),
        ..output_config
    };

    assert_eq!(input_config.channels, output_config.channels);
    assert_eq!(input_config.sample_rate, output_config.sample_rate);

    // Create the channel for synchronization.
    let (mut sender, mut receiver) = channel(2 << 14, || 0.0);

    // Input callback copies to the channel.
    let input_callback = move |mut data: &[f32], _info: &InputCallbackInfo| {
        for _ in 0..2 {
            // If this returns an error it means the output callback dropped.
            let Ok(mut guard) = sender.try_send() else {
                return;
            };

            // Copy to the channel.
            let length = guard.len().min(data.len());
            guard[0..length].copy_from_slice(&data[0..length]);

            // Commit the data.
            guard.truncate(length);
            guard.commit();

            // Update data and bail if needed.
            data = &data[length..];
            if data.is_empty() {
                break;
            }
        }
    };

    // Output callback copies out of the channel.
    let output_callback = move |mut data: &mut [f32], _info: &OutputCallbackInfo| {
        data.fill(0.0);
        for _ in 0..2 {
            // If this returns an error it means the input callback dropped.
            let Ok(mut guard) = receiver.try_recv() else {
                return;
            };

            // Copy out of the channel.
            let length = guard.len().min(data.len());
            data[0..length].copy_from_slice(&guard[0..length]);

            // Decommit the data.
            guard.truncate(length);
            guard.decommit();

            // Update data and bail if needed.
            data = &mut data[length..];
            if data.is_empty() {
                break;
            }
        }
    };

    let _input_stream = input_device
        .build_input_stream(&input_config, input_callback, error_callback, None)
        .unwrap();
    let _output_stream = output_device
        .build_output_stream(&output_config, output_callback, error_callback, None)
        .unwrap();

    std::thread::sleep(std::time::Duration::from_secs(5));
}

fn error_callback(_error: StreamError) {}
