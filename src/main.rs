mod models;
mod consumer;
mod mailer;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    if let Err(e) = mailer::init_mailer() {
        tracing::error!(?e, "Falha ao inicializar mailer — verifique SMTP_*");
        std::process::exit(1);
    }

    if let Err(e) = consumer::start_consumer().await {
        tracing::error!(?e, "Erro fatal ao conectar no RabbitMQ");
        std::process::exit(1);
    }
}