use std::{pin::Pin, task::Poll};

use async_io::Async;
use futures_core::Stream;
use futures_sink::Sink;

pub struct ZmqStream {
    aio: Async<zmq::Socket>,
    buffer: Option<Vec<u8>>,
}

impl ZmqStream {
    pub fn new(socket: zmq::Socket) -> std::io::Result<Self> {
        Ok(Self {
            aio: Async::new(socket)?,
            buffer: None,
        })
    }

    fn setup_listener(&self, cx: &mut std::task::Context<'_>) -> Result<(), std::io::Error> {
        while let Poll::Ready(r) = self.aio.poll_readable(cx) {
            r?
        }
        Ok(())
    }
}

impl Stream for ZmqStream {
    type Item = std::io::Result<Vec<u8>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Err(e) = self.setup_listener(cx) {
            return Poll::Ready(Some(Err(e)));
        }
        match self.aio.as_ref().recv_bytes(zmq::DONTWAIT) {
            Err(zmq::Error::EAGAIN) => Poll::Pending,
            r => Poll::Ready(Some(r.map_err(Into::into))),
        }
    }
}

// See https://docs.rs/async_zmq/0.4.0/src/async_zmq/socket.rs.html#105-129

impl Sink<Vec<u8>> for ZmqStream {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        self.get_mut().buffer = Some(item);
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if let Err(e) = self.setup_listener(cx) {
            return Poll::Ready(Err(e));
        }
        if let Some(ref msg) = self.buffer {
            let poll = match self.aio.as_ref().send(msg.clone(), zmq::DONTWAIT) {
                Err(zmq::Error::EAGAIN) => return Poll::Pending,
                r => Poll::Ready(r.map_err(Into::into)),
            };
            self.get_mut().buffer = None;
            poll
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

impl Unpin for ZmqStream {}
