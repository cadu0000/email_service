use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use tokio_stream::StreamExt;
use crate::models::NotificationPayload;
use crate::mailer::send_email;

pub async fn start_consumer() -> lapin::Result<()> {
    let addr = std::env::var("RABBITMQ_URL")
        .expect("RABBITMQ_URL environment variable must be set");

    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    let queue_name = "transaction.notification.queue";

    channel.queue_declare(queue_name, QueueDeclareOptions::default(), FieldTable::default()).await?;
    channel.basic_qos(10, BasicQosOptions::default()).await?;

    let mut consumer = channel.basic_consume(
        queue_name,
        "notification_worker",
        BasicConsumeOptions::default(),
        FieldTable::default(),
    ).await?;

    tracing::info!(queue = queue_name, "Worker aguardando mensagens");

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("Sinal de encerramento recebido, finalizando consumo...");
                break;
            }
            maybe_delivery = consumer.next() => {
                match maybe_delivery {
                    Some(Ok(delivery)) => handle_delivery(delivery).await,
                    Some(Err(err)) => tracing::error!(?err, "Erro ao receber delivery"),
                    None => break,
                }
            }
        }
    }

    Ok(())
}

async fn handle_delivery(delivery: lapin::message::Delivery) {
    match serde_json::from_slice::<NotificationPayload>(&delivery.data) {
        Ok(payload) => match send_email(&payload).await {
            Ok(_) => {
                if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                    tracing::error!(?err, "Falha ao confirmar (ack) mensagem");
                }
            }
            Err(err) => {
                tracing::error!(?err, email = %payload.email, "Falha ao enviar e-mail; reenfileirando");
                let _ = delivery.nack(BasicNackOptions { requeue: true, ..Default::default() }).await;
            }
        },
        Err(err) => {
            tracing::warn!(?err, "JSON malformado na fila, descartando");
            let _ = delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await;
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("falha ao instalar handler Ctrl+C");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("falha ao instalar handler SIGTERM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}