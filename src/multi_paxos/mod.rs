mod commands;
mod replica;
pub use self::commands::*;
pub use self::replica::Replica;

pub type Slot = u64;
