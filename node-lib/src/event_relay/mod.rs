// Copyright (c) 2024 RBB S.r.l
// opensource@mintlayer.org
// SPDX-License-Identifier: MIT
// Licensed under the MIT License;
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://github.com/mintlayer/mintlayer-core/blob/master/LICENSE
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! WebSocket event relay: broadcasts mempool and chainstate events to connected clients.

use std::net::SocketAddr;

use axum::{
    extract::{
        ws::{Message, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use chainstate::ChainstateHandle;
use common::{
    chain::{GenBlock, Transaction},
    primitives::Id,
};
use logging::log;
use mempool::MempoolHandle;
use serialization::hex::HexEncode;
use tokio::sync::{broadcast, oneshot};

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsEvent {
    NewTx {
        tx_id: String,
        raw_tx: String,
    },
    NewBlock {
        block_id: String,
        height: u64,
    },
}

pub struct EventRelayServer {
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[async_trait::async_trait]
impl subsystem::Subsystem for EventRelayServer {
    type Interface = Self;

    fn interface_ref(&self) -> &Self {
        self
    }

    fn interface_mut(&mut self) -> &mut Self {
        self
    }

    async fn shutdown(self) {
        self.shutdown().await
    }
}

impl EventRelayServer {
    pub async fn start(
        bind_address: SocketAddr,
        chainstate: ChainstateHandle,
        mempool: MempoolHandle,
    ) -> anyhow::Result<Self> {
        let (broadcast_tx, _) = broadcast::channel::<WsEvent>(1024);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Subscribe to subsystem events inside the spawned task so start() returns
        // immediately. Awaiting subsystem handles here would deadlock because the
        // chainstate/mempool tasks are not yet spawned when init_node() calls start().
        let task = tokio::spawn(async move {
            let chain_rx = match chainstate
                .call_mut(|cs| cs.subscribe_to_rpc_events())
                .await
            {
                Ok(rx) => rx,
                Err(e) => {
                    log::error!("Event relay: failed to subscribe to chainstate events: {e}");
                    return;
                }
            };

            let mempool_rx = match mempool
                .call_mut(|m| m.subscribe_to_rpc_events())
                .await
            {
                Ok(rx) => rx,
                Err(e) => {
                    log::error!("Event relay: failed to subscribe to mempool events: {e}");
                    return;
                }
            };

            run_event_relay(bind_address, broadcast_tx, chain_rx, mempool_rx, mempool, shutdown_rx)
                .await;
        });

        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }

    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

async fn run_event_relay(
    bind_address: SocketAddr,
    broadcast_tx: broadcast::Sender<WsEvent>,
    mut chain_rx: utils_networking::broadcaster::Receiver<chainstate::ChainstateEvent>,
    mut mempool_rx: utils_networking::broadcaster::Receiver<mempool::event::MempoolEvent>,
    mempool: MempoolHandle,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(broadcast_tx.clone());

    let listener = match tokio::net::TcpListener::bind(bind_address).await {
        Ok(l) => l,
        Err(e) => {
            log::error!("Event relay: failed to bind to {bind_address}: {e}");
            return;
        }
    };

    log::info!("Event relay WebSocket server listening on {bind_address}");

    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        loop {
            tokio::select! {
                event = chain_rx.recv() => {
                    match event {
                        Some(chainstate::ChainstateEvent::NewTip { id, height, .. }) => {
                            let event = WsEvent::NewBlock {
                                block_id: format_block_id(id),
                                height: height.into_int(),
                            };
                            let _ = broadcast_tx.send(event);
                        }
                        None => break,
                    }
                }
                event = mempool_rx.recv() => {
                    match event {
                        Some(mempool::event::MempoolEvent::TransactionProcessed(e)) => {
                            if e.result().is_ok() {
                                let tx_id = *e.tx_id();
                                let mempool_clone = mempool.clone();
                                let broadcast_clone = broadcast_tx.clone();
                                tokio::spawn(async move {
                                    fetch_and_broadcast_tx(tx_id, mempool_clone, broadcast_clone).await;
                                });
                            }
                        }
                        Some(mempool::event::MempoolEvent::NewTip(_)) => {}
                        None => break,
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    if let Err(e) = server.await {
        log::error!("Event relay server error: {e}");
    }
}

async fn fetch_and_broadcast_tx(
    tx_id: Id<Transaction>,
    mempool: MempoolHandle,
    broadcast_tx: broadcast::Sender<WsEvent>,
) {
    let result = mempool
        .call(move |m| m.transaction(&tx_id))
        .await;

    match result {
        Ok(Some(tx)) => {
            let raw_tx = tx.hex_encode();
            let event = WsEvent::NewTx {
                tx_id: format!("{:x}", tx_id),
                raw_tx,
            };
            let _ = broadcast_tx.send(event);
        }
        Ok(None) => {
            // tx was evicted from mempool before we could fetch it
        }
        Err(e) => {
            log::warn!("Event relay: failed to fetch tx from mempool: {e}");
        }
    }
}

fn format_block_id(id: Id<GenBlock>) -> String {
    format!("{:x}", id)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(broadcast_tx): State<broadcast::Sender<WsEvent>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, broadcast_tx))
}

async fn handle_ws(
    mut socket: axum::extract::ws::WebSocket,
    broadcast_tx: broadcast::Sender<WsEvent>,
) {
    let mut rx = broadcast_tx.subscribe();

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(ws_event) => {
                        let json = match serde_json::to_string(&ws_event) {
                            Ok(j) => j,
                            Err(e) => {
                                log::warn!("Event relay: failed to serialize event: {e}");
                                continue;
                            }
                        };
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("Event relay: WebSocket client lagged, dropped {n} events");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
        }
    }
}
