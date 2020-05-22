mod commands;
mod replica;
pub use self::replica::Replica;
pub use self::commands::*;

pub type Slot = u64;
