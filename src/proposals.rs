use std::io;
use futures::{Poll, Sink, StartSend, Stream};
use futures::unsync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use algo::Value;

/// Creates a sink and stream pair for proposals.
pub fn proposal_channel() -> (ProposalSender, ProposalReceiver) {
    // TODO: bound the number of in-flight proposals, possible batching
    let (sink, stream) = unbounded::<Value>();
    (ProposalSender { sink }, ProposalReceiver { stream })
}


/// Stream for consuming proposals.
pub struct ProposalReceiver {
    stream: UnboundedReceiver<Value>,
}

impl Stream for ProposalReceiver {
    type Item = Value;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Value>, io::Error> {
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
    sink: UnboundedSender<Value>,
}

impl ProposalSender {
    /// Sends a proposal to the current node.
    pub fn propose(&self, value: Value) -> io::Result<()> {
        self.sink.unbounded_send(value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with unbounded sender for proposal",
            )
        })
    }
}

impl Sink for ProposalSender {
    type SinkItem = Value;
    type SinkError = io::Error;

    fn start_send(&mut self, value: Value) -> StartSend<Value, io::Error> {
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
