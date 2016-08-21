extern crate capnpc;

fn main() {
    ::capnpc::compile(".", &["messages.capnp"]).unwrap();
}
