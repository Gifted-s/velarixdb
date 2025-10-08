use crate::types::Key;
use std::collections::HashSet;
// The start of isolation level on Velarix

#[derive(Debug, Clone)]
pub enum IsolationLevel {
    ReadCommitted,
    ReadUncommitted,
    RepeatableRead,
    Serializable,
}

struct TranactionState {
    pub isolation_level: IsolationLevel,
    pub read_set: HashSet<Key>,
    pub write_set: HashSet<Key>,
    pub read_version: u64, // The version of the reads
    pub write_version: u64, // Write version of write set
    pub read_timestamp: u64, // Timestamp of the reads
    pub write_timestamp: u64, // Timestamp of the writes
    pub is_active:bool, // State of transaction 
    pub committed: bool, // Whether the transaction has been committed
    pub aborted: bool,   // Whether the transaction has been aborted
    pub savepoint: Option<u64>, // Optional savepoint for partial rollbacks
    pub parent_id: Option<u64>, // Optional parent transaction id for nested transactions
    pub transaction_id: u64,    // Unique transaction identifier
}

pub struct Transaction {
    pub isolation_level: IsolationLevel,
}