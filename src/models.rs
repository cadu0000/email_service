use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct NotificationPayload {
    pub email: String,
    #[serde(rename = "tipo")]
    pub transaction_type: String,
    #[serde(rename = "valor")]
    pub amount: f64,
    #[serde(rename = "descricao")]
    pub description: String,
}