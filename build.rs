extern crate capnpc;

fn main() {
    ::capnpc::compile("schema", &["schema/messages.capnp"]).unwrap();
}
