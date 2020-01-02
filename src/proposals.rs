use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use futures::stream::Stream;
use futures::sink::Sink;
use std::pin::Pin;
use std::io;
use bytes::Bytes;
use std::task::{Context, Poll};
use pin_project::pin_project;

/// Creates a sink and stream pair for proposals.
pub fn proposal_channel() -> (ProposalSender, ProposalReceiver) {
    // TODO: bound the number of in-flight proposals, possible batching
    let (sink, stream) = unbounded_channel::<Bytes>();
    (ProposalSender { sink }, ProposalReceiver { stream })
}

/// Stream for consuming proposals.
#[pin_project]
pub struct ProposalReceiver {
    #[pin]
    stream: UnboundedReceiver<Bytes>,
}

impl Stream for ProposalReceiver {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Bytes>> {
        self.project().stream.poll_next(cx)
    }
}

/// Sink allowing proposals to be sent asynchronously.
#[pin_project]
#[derive(Clone)]
pub struct ProposalSender {
    #[pin]
    sink: UnboundedSender<Bytes>,
}

impl ProposalSender {
    /// Sends a proposal to the current node.
    pub fn propose(&self, value: Bytes) -> io::Result<()> {
        self.project().sink.send(value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with unbounded sender for proposal",
            )
        })
    }
}

impl Sink<Bytes> for ProposalSender {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.sink.poll_ready(cx).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with poll_ready on sender for proposal",
            )
        })
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), io::Error> {
        self.sink.start_send(item).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with start_send on sender for proposal",
            )
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.sink.poll_flush(cx).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with poll_flush on sender for proposal",
            )
        })
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.sink.poll_close(cx).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with poll_close on sender for proposal",
            )
        })
    }
}
