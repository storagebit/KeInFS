// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::NotificationMode;
use crate::stats::KmsStats;
use futures_util::StreamExt;
use keinctl::proto::MetadataInvalidationEvent;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct NotificationEvent {
    pub(crate) sequence: u64,
    pub(crate) namespace_id: String,
    pub(crate) bucket_id: String,
    pub(crate) key: String,
}

#[derive(Clone)]
pub(crate) struct NotificationHub {
    tx: watch::Sender<NotificationEvent>,
    publish_tx: Option<mpsc::UnboundedSender<MetadataInvalidationEvent>>,
}

impl NotificationHub {
    pub(crate) fn spawn(
        subject: String,
        nats_url: String,
        mode: NotificationMode,
        poll_interval: Duration,
        stats: Arc<KmsStats>,
    ) -> NotificationHub {
        let (tx, _) = watch::channel(NotificationEvent::default());
        let loop_tx = tx.clone();
        let mut publish_tx = None;
        match mode {
            NotificationMode::Nats => {
                let (publisher_tx, publisher_rx) = mpsc::unbounded_channel();
                publish_tx = Some(publisher_tx);
                let publisher_nats_url = nats_url.clone();
                let publisher_subject = subject.clone();
                let publisher_stats = stats.clone();
                tokio::spawn(async move {
                    nats_publisher_loop(
                        publisher_nats_url,
                        publisher_subject,
                        publisher_rx,
                        publisher_stats,
                    )
                    .await;
                });
                tokio::spawn(async move {
                    nats_listener_loop(nats_url, subject, loop_tx, stats).await;
                });
            }
            NotificationMode::Poll => {
                tokio::spawn(async move {
                    poll_loop(loop_tx, poll_interval, stats).await;
                });
            }
        }
        NotificationHub { tx, publish_tx }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<NotificationEvent> {
        self.tx.subscribe()
    }

    pub(crate) fn notify(&self, event: MetadataInvalidationEvent) {
        Self::poke_event(&self.tx, event_to_notification(&event));
        if let Some(publish_tx) = &self.publish_tx {
            let _ = publish_tx.send(event);
        }
    }

    fn poke_event(tx: &watch::Sender<NotificationEvent>, mut next: NotificationEvent) {
        next.sequence = tx.borrow().sequence.saturating_add(1);
        let _ = tx.send(next);
    }
}

fn event_to_notification(event: &MetadataInvalidationEvent) -> NotificationEvent {
    NotificationEvent {
        sequence: 0,
        namespace_id: event.namespace_id.clone(),
        bucket_id: event.bucket_id.clone(),
        key: event.key.clone(),
    }
}

fn decode_notification_payload(payload: &[u8]) -> NotificationEvent {
    if let Ok(event) = MetadataInvalidationEvent::decode(payload) {
        return NotificationEvent {
            sequence: 0,
            namespace_id: event.namespace_id,
            bucket_id: event.bucket_id,
            key: event.key,
        };
    }
    let namespace_id = std::str::from_utf8(payload)
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    NotificationEvent {
        sequence: 0,
        namespace_id,
        bucket_id: String::new(),
        key: String::new(),
    }
}

async fn nats_listener_loop(
    nats_url: String,
    subject: String,
    tx: watch::Sender<NotificationEvent>,
    stats: Arc<KmsStats>,
) {
    loop {
        match async_nats::connect(&nats_url).await {
            Ok(client) => match client.subscribe(subject.clone()).await {
                Ok(mut subscriber) => {
                    NotificationHub::poke_event(&tx, NotificationEvent::default());
                    while let Some(message) = subscriber.next().await {
                        NotificationHub::poke_event(
                            &tx,
                            decode_notification_payload(message.payload.as_ref()),
                        );
                    }
                    stats
                        .set_last_error("KMS notification listener NATS stream closed".to_string());
                }
                Err(err) => {
                    stats.set_last_error(format!(
                        "KMS notification listener could not subscribe to {} on {}: {}",
                        subject, nats_url, err
                    ));
                }
            },
            Err(err) => stats.set_last_error(format!(
                "KMS notification listener could not connect to NATS {}: {}",
                nats_url, err
            )),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn nats_publisher_loop(
    nats_url: String,
    subject: String,
    mut rx: mpsc::UnboundedReceiver<MetadataInvalidationEvent>,
    stats: Arc<KmsStats>,
) {
    let mut client: Option<async_nats::Client> = None;
    while let Some(event) = rx.recv().await {
        let payload = event.encode_to_vec();
        loop {
            if client.is_none() {
                match async_nats::connect(&nats_url).await {
                    Ok(connected) => client = Some(connected),
                    Err(err) => {
                        stats.set_last_error(format!(
                            "KMS notification publisher could not connect to NATS {}: {}",
                            nats_url, err
                        ));
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
            let Some(connected) = client.as_ref() else {
                continue;
            };
            match connected
                .publish(subject.clone(), payload.clone().into())
                .await
            {
                Ok(()) => break,
                Err(err) => {
                    stats.set_last_error(format!(
                        "KMS notification publisher could not publish to {} on {}: {}",
                        subject, nats_url, err
                    ));
                    client = None;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

async fn poll_loop(tx: watch::Sender<NotificationEvent>, interval: Duration, stats: Arc<KmsStats>) {
    stats.set_last_error(String::new());
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        NotificationHub::poke_event(&tx, NotificationEvent::default());
    }
}
