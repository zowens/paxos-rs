use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::{Poll, Sink, StartSend, Stream};
use std::io;
use bytes::Bytes;

/// Creates a sink and stream pair for proposals.
pub fn proposal_channel() -> (ProposalSender, ProposalReceiver) {
    // TODO: bound the number of in-flight proposals, possible batching
    let (sink, stream) = unbounded::<Bytes>();
    (ProposalSender { sink }, ProposalReceiver { stream })
}

/// Stream for consuming proposals.
pub struct ProposalReceiver {
    stream: UnboundedReceiver<Bytes>,
}

impl Stream for ProposalReceiver {
    type Item = Bytes;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Bytes>, io::Error> {
        self.stream.poll().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with pollon unbounded receiver for proposals",
            )
        })
    }
}

/// Sink allowing proposals to be sent asynchronously.
#[derive(Clone)]
pub struct ProposalSender {
    sink: UnboundedSender<Bytes>,
}

impl ProposalSender {
    /// Sends a proposal to the current node.
    pub fn propose(&self, value: Bytes) -> io::Result<()> {
        self.sink.unbounded_send(value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with unbounded sender for proposal",
            )
        })
    }
}

impl Sink for ProposalSender {
    type SinkItem = Bytes;
    type SinkError = io::Error;

    fn start_send(&mut self, value: Bytes) -> StartSend<Bytes, io::Error> {
        self.sink.start_send(value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with start_send on unbounded sender for proposal",
            )
        })
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.sink.poll_complete().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with poll_complete on unbounded sender for proposal",
            )
        })
    }
}
