use lettre::{
    message::header::ContentType,
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use std::sync::OnceLock;
use crate::models::NotificationPayload;

static MAILER: OnceLock<AsyncSmtpTransport<Tokio1Executor>> = OnceLock::new();

pub fn init_mailer() -> anyhow::Result<()> {
    let host = std::env::var("SMTP_HOST")?;
    let user = std::env::var("SMTP_USER")?;
    let pass = std::env::var("SMTP_PASSWORD")?;

    let transport = AsyncSmtpTransport::<Tokio1Executor>::relay(&host)?
        .credentials(Credentials::new(user, pass))
        .build();

    MAILER
        .set(transport)
        .map_err(|_| anyhow::anyhow!("mailer já inicializado"))
}

pub async fn send_email(payload: &NotificationPayload) -> anyhow::Result<()> {
    let mailer = MAILER
        .get()
        .ok_or_else(|| anyhow::anyhow!("mailer não inicializado — chame init_mailer() no startup"))?;

    let formatted_amount = format!("{:.2}", payload.amount).replace('.', ",");
    let body = format!(
        "Tipo: {}\nValor: R$ {}\nDescrição: {}",
        payload.transaction_type, formatted_amount, payload.description
    );
    let from = std::env::var("SMTP_FROM").unwrap_or_else(|_| "no-reply@seuapp.com".to_string());

    let email = Message::builder()
        .from(from.parse()?)
        .to(payload.email.parse()?)
        .subject("Nova transação registrada")
        .header(ContentType::TEXT_PLAIN)
        .body(body)?;

    mailer.send(email).await?;
    Ok(())
}