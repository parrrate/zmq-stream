use std::time::Duration;

use async_io::{block_on, Timer};
use futures_util::{future::join, SinkExt, StreamExt};
use zmq_stream::ZmqStream;

fn main() -> std::io::Result<()> {
    let ctx = zmq::Context::new();
    let publisher = ctx.socket(zmq::PAIR)?;
    publisher.set_sndhwm(2)?;
    publisher.bind("inproc://0")?;
    let consumer = ctx.socket(zmq::PAIR)?;
    consumer.set_rcvhwm(2)?;
    consumer.connect(&publisher.get_last_endpoint()?.unwrap())?;
    let mut publisher = ZmqStream::new(publisher)?;
    let mut consumer = ZmqStream::new(consumer)?;
    let publish = async move {
        let mut timer = Timer::interval(Duration::from_millis(100));
        for i in 0.. {
            println!("sending {i}");
            publisher.send(format!("{i}").into()).await.unwrap();
            println!("sent {i}");
            timer.next().await;
        }
    };
    let consume = async move {
        Timer::after(Duration::from_millis(1000)).await;
        loop {
            println!("receiving");
            let msg = String::from_utf8(consumer.next().await.unwrap().unwrap()).unwrap();
            println!("received {msg}");
        }
    };
    block_on(join(publish, consume));
    Ok(())
}
