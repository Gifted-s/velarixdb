
// The start of isolation level on Velarix
#[derive(Debug, Clone)]
pub enum IsolationLevel {
    ReadCommitted,
    ReadUncommitted,
    RepeatableRead,
    Serializable,
}

struct TranactionState {
    
}

pub struct Transaction {
    pub isolation_level: IsolationLevel,
    
}