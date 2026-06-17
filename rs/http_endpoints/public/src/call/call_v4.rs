use std::sync::{Arc, RwLock};

use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    Json, Router,
};
use ic_artifact_pool::consensus_pool::ConsensusPoolImpl;
use ic_config::artifact_pool::PersistentPoolBackend;
use ic_interfaces_state_manager::StateReader;
use ic_replicated_state::ReplicatedState;
use ic_types::{
    consensus::{Block, HashedBlock},
    crypto::crypto_hash,
    CanisterId, Height,
};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;

pub(crate) fn route() -> &'static str {
    "/api/v4"
}

pub(crate) fn new_router(
    state_reader: Arc<dyn StateReader<State = ReplicatedState>>,
    consensus_pool: Arc<RwLock<ConsensusPoolImpl>>,
    _artifact_pool_config: PersistentPoolBackend,
) -> Router {
    Router::new()
        .route_service(
            "/api/v4/height",
            axum::routing::get(get_height)
                .with_state(state_reader.clone())
                .layer(ServiceBuilder::new().layer(DefaultBodyLimit::disable())),
        )
        .route_service(
            "/api/v4/block/:height",
            axum::routing::get(get_block_at)
                .with_state(consensus_pool.clone())
                .layer(ServiceBuilder::new().layer(DefaultBodyLimit::disable())),
        )
        .route_service(
            "/api/v4/blocks",
            axum::routing::get(list_blocks)
                .with_state(consensus_pool.clone())
                .layer(ServiceBuilder::new().layer(DefaultBodyLimit::disable())),
        )
}

#[derive(Serialize)]
struct GetHeight {
    height: u64,
}

async fn get_height(
    State(state): State<Arc<dyn StateReader<State = ReplicatedState>>>,
) -> Json<GetHeight> {
    Json(GetHeight {
        height: state.latest_state_height().get(),
    })
}

#[derive(Serialize)]
#[serde(untagged)]
enum CallResponse {
    Block(GetBlock),
    Blocks(Vec<GetBlock>),
    Err(ErrMsg),
}

#[derive(Serialize)]
struct ErrMsg {
    message: String,
}

#[derive(Serialize)]
struct GetBlock {
    prev_hash: String,
    height: u64,
    block_hash: String,
    time: u64,
    ingress_count: usize,
    upsert_count: usize,
    ingress_messages: Option<Vec<IngressMessage>>,
}

#[derive(Serialize)]
struct IngressMessage {
    message_id: String,
    canister_id: CanisterId,
    method_name: String,
    sender: String,
}

impl From<&Block> for GetBlock {
    fn from(block: &Block) -> Self {
        let prev_hash = format!("0x{}", hex::encode(block.parent.clone().get().0));
        let height = block.height.get();
        let hb = HashedBlock::new(crypto_hash, block.clone());
        let block_hash = format!("0x{}", hex::encode(hb.get_hash().clone().get().0));
        let time = block.context.time.as_nanos_since_unix_epoch();
        Self {
            prev_hash,
            height,
            block_hash,
            time,
            ingress_count: 0,
            upsert_count: 0,
            ingress_messages: None,
        }
    }
}

fn extract_block(pool: &ConsensusPoolImpl, height: Height) -> Option<Block> {
    let finalization = pool.validated.finalization().get_only_by_height(height).ok()?;
    let block_hash = &finalization.content.block;
    pool.validated
        .block_proposal()
        .get_by_height(height)
        .find(|bp| bp.content.get_hash() == block_hash)
        .map(|bp| bp.content.clone().into_inner())
}

fn block_to_getblock(blk: &Block, with_messages: bool) -> GetBlock {
    let mut gb = GetBlock::from(blk);
    if !blk.payload.is_summary() {
        let batch = &blk.payload.as_ref().as_data().batch;
        let count = batch.ingress.message_count();
        gb.ingress_count = count;
        let mut upsert = 0usize;
        let mut msgs = vec![];
        for i in 0..count {
            if let Ok((message_id, message)) = batch.ingress.get(i) {
                let c = message.as_ref().content();
                let method = c.method_name().to_string();
                if method == "upsert_vote" {
                    upsert += 1;
                }
                if with_messages {
                    msgs.push(IngressMessage {
                        message_id: format!("0x{}", message_id.message_id),
                        canister_id: c.canister_id(),
                        method_name: method,
                        sender: c.sender().get().0.to_text(),
                    });
                }
            }
        }
        gb.upsert_count = upsert;
        if with_messages && !msgs.is_empty() {
            gb.ingress_messages = Some(msgs);
        }
    }
    gb
}

async fn get_block_at(
    Path(height): Path<u64>,
    State(consensus_pool): State<Arc<RwLock<ConsensusPoolImpl>>>,
) -> Json<CallResponse> {
    let pool = consensus_pool.read().expect("read pool");
    match extract_block(&pool, Height::new(height)) {
        Some(blk) => Json(CallResponse::Block(block_to_getblock(&blk, true))),
        None => Json(CallResponse::Err(ErrMsg {
            message: "Block not found".to_string(),
        })),
    }
}

#[derive(Deserialize)]
struct BlockRange {
    from: u64,
    to: u64,
}

async fn list_blocks(
    Query(range): Query<BlockRange>,
    State(consensus_pool): State<Arc<RwLock<ConsensusPoolImpl>>>,
) -> Json<CallResponse> {
    let pool = consensus_pool.read().expect("read pool");
    let mut blocks = vec![];
    for h in range.from..=range.to {
        if let Some(blk) = extract_block(&pool, Height::new(h)) {
            blocks.push(block_to_getblock(&blk, false));
        }
    }
    Json(CallResponse::Blocks(blocks))
}
