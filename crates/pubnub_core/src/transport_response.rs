use std::collections::HashMap;

#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct TransportResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Option<Vec<u8>>,
}
