mod inmemory;
mod val_option;
pub(crate) use inmemory::InMemoryTable;
pub(crate) use inmemory::Entry;
pub(crate) use inmemory::{DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MEMTABLE_CAPACITY};