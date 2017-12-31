use std::io;
use futures::{Poll, Sink, StartSend, Stream};
use futures::unsync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use algo::Value;

/// Creates a sink and stream pair for proposals.
pub fn proposal_channel<V: Value>() -> (ProposalSender<V>, ProposalReceiver<V>) {
    // TODO: bound the number of in-flight proposals, possible batching
    let (sink, stream) = unbounded::<V>();
    (ProposalSender { sink }, ProposalReceiver { stream })
}


/// Stream for consuming proposals.
pub struct ProposalReceiver<V: Value> {
    stream: UnboundedReceiver<V>,
}

impl<V: Value> Stream for ProposalReceiver<V> {
    type Item = V;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<V>, io::Error> {
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
pub struct ProposalSender<V: Value> {
    sink: UnboundedSender<V>,
}

impl<V: Value> ProposalSender<V> {
    /// Sends a proposal to the current node.
    pub fn propose(&self, value: V) -> io::Result<()> {
        self.sink.unbounded_send(value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with unbounded sender for proposal",
            )
        })
    }
}

impl<V: Value> Sink for ProposalSender<V> {
    type SinkItem = V;
    type SinkError = io::Error;

    fn start_send(&mut self, value: V) -> StartSend<V, io::Error> {
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
