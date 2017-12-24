
type TimerSink<T> = ();

type TimerStream<T> = ();


pub fn interval<T>(handle: Handle, duration: Duration, value: T) -> TimerStream<T> {

}

pub fn delayed<T>(handle: Handle) -> (TimerSink<T>, TimerStream<T>) {

}


