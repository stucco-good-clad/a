use std::sync::Arc;

use clickhouse::Client;
use dashmap::DashMap;
use jetstreamer::{
    firehose::{BlockData, TransactionData},
    plugin::{Plugin, PluginFuture},
};
use serde::Serialize;
use solana_message::VersionedMessage;

const DEX_PROGRAMS: &[&str] = &[
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
];

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

#[derive(Serialize, Clone)]
struct EnrichedBlock {
    #[serde(rename = "blockhash")]
    blockhash: String,
    #[serde(rename = "blockTime")]
    block_time: Option<i64>,
    #[serde(rename = "parentSlot")]
    parent_slot: u64,
    #[serde(rename = "transactions")]
    transactions: Vec<EnrichedTransaction>,
}

#[derive(Serialize, Clone)]
struct EnrichedTransaction {
    #[serde(rename = "transaction")]
    transaction: SimpleTransaction,
    #[serde(rename = "meta")]
    meta: TransactionMeta,
}

#[derive(Serialize, Clone)]
struct SimpleTransaction {
    #[serde(rename = "message")]
    message: SimpleMessage,
    #[serde(rename = "signatures")]
    signatures: Vec<String>,
}

#[derive(Serialize, Clone)]
struct SimpleMessage {
    #[serde(rename = "accountKeys")]
    account_keys: Vec<String>,
    #[serde(rename = "instructions")]
    instructions: Vec<SimpleInstruction>,
    #[serde(rename = "recentBlockhash")]
    recent_blockhash: String,
}

#[derive(Serialize, Clone)]
struct SimpleInstruction {
    #[serde(rename = "programIdIndex")]
    program_id_index: u8,
    #[serde(rename = "accounts")]
    accounts: Vec<u8>,
    #[serde(rename = "data")]
    data: String,
}

#[derive(Serialize, Clone)]
struct TransactionMeta {
    #[serde(rename = "fee")]
    fee: u64,
    #[serde(rename = "preBalances")]
    pre_balances: Vec<u64>,
    #[serde(rename = "postBalances")]
    post_balances: Vec<u64>,
    #[serde(rename = "innerInstructions")]
    inner_instructions: Vec<InnerInstructionSet>,
    #[serde(rename = "logMessages")]
    log_messages: Vec<String>,
    #[serde(rename = "preTokenBalances")]
    pre_token_balances: Vec<TokenBalance>,
    #[serde(rename = "postTokenBalances")]
    post_token_balances: Vec<TokenBalance>,
}

#[derive(Serialize, Clone)]
struct InnerInstructionSet {
    #[serde(rename = "index")]
    index: u8,
    #[serde(rename = "instructions")]
    instructions: Vec<SimpleInstruction>,
}

#[derive(Serialize, Clone)]
struct TokenBalance {
    #[serde(rename = "accountIndex")]
    account_index: u8,
    #[serde(rename = "mint")]
    mint: String,
    #[serde(rename = "uiTokenAmount")]
    ui_token_amount: UiTokenAmount,
    #[serde(rename = "owner")]
    owner: String,
}

#[derive(Serialize, Clone)]
struct UiTokenAmount {
    #[serde(rename = "amount")]
    amount: String,
    #[serde(rename = "decimals")]
    decimals: u8,
    #[serde(rename = "uiAmount")]
    ui_amount: Option<f64>,
    #[serde(rename = "uiAmountString")]
    ui_amount_string: String,
}

pub struct DexSwapPlugin {
    output_dir: String,
    pending: DashMap<u64, Vec<EnrichedTransaction>>,
}

impl DexSwapPlugin {
    pub fn new(output_dir: String) -> Self {
        let _ = std::fs::create_dir_all(&output_dir);
        Self {
            output_dir,
            pending: DashMap::new(),
        }
    }

    fn extract_program_ids(tx: &TransactionData) -> Vec<String> {
        let message = &tx.transaction.message;
        let (keys, ixs) = match message {
            VersionedMessage::Legacy(m) => (&m.account_keys, &m.instructions),
            VersionedMessage::V0(m) => (&m.account_keys, &m.instructions),
        };
        ixs.iter()
            .filter_map(|ix| keys.get(ix.program_id_index as usize).map(|k| k.to_string()))
            .collect()
    }

    fn is_dex(tx: &TransactionData) -> bool {
        Self::extract_program_ids(tx)
            .iter()
            .any(|pid| DEX_PROGRAMS.contains(&pid.as_str()))
    }

    fn has_sol_usdc_usdt(meta: &solana_transaction_status::TransactionStatusMeta) -> bool {
        let check = |balances: &Option<Vec<solana_transaction_status::TransactionTokenBalance>>| {
            balances
                .as_ref()
                .map(|bs| {
                    bs.iter()
                        .any(|b| b.mint == SOL_MINT || b.mint == USDC_MINT || b.mint == USDT_MINT)
                })
                .unwrap_or(false)
        };
        check(&meta.pre_token_balances) || check(&meta.post_token_balances)
    }

    fn enrich(tx: &TransactionData) -> EnrichedTransaction {
        let meta = &tx.transaction_status_meta;

        let inner_instructions = meta
            .inner_instructions
            .as_ref()
            .map(|sets| {
                sets.iter()
                    .map(|set| InnerInstructionSet {
                        index: set.index,
                        instructions: set
                            .instructions
                            .iter()
                            .map(|ix| SimpleInstruction {
                                program_id_index: ix.instruction.program_id_index,
                                accounts: ix.instruction.accounts.clone(),
                                data: const_hex::encode(&ix.instruction.data),
                            })
                            .collect(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let convert_balances =
            |balances: &Option<Vec<solana_transaction_status::TransactionTokenBalance>>| {
                balances
                    .as_ref()
                    .map(|bs| {
                        bs.iter()
                            .map(|b| TokenBalance {
                                account_index: b.account_index,
                                mint: b.mint.clone(),
                                ui_token_amount: UiTokenAmount {
                                    amount: b.ui_token_amount.amount.clone(),
                                    decimals: b.ui_token_amount.decimals,
                                    ui_amount: b.ui_token_amount.ui_amount,
                                    ui_amount_string: b.ui_token_amount.ui_amount_string.clone(),
                                },
                                owner: b.owner.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };

        let log_messages = meta.log_messages.clone().unwrap_or_default();

        let message = &tx.transaction.message;
        let (account_keys, instructions, recent_blockhash) = match message {
            VersionedMessage::Legacy(m) => (
                m.account_keys.iter().map(|k| k.to_string()).collect(),
                m.instructions
                    .iter()
                    .map(|ix| SimpleInstruction {
                        program_id_index: ix.program_id_index,
                        accounts: ix.accounts.clone(),
                        data: const_hex::encode(&ix.data),
                    })
                    .collect(),
                m.recent_blockhash.to_string(),
            ),
            VersionedMessage::V0(m) => (
                m.account_keys.iter().map(|k| k.to_string()).collect(),
                m.instructions
                    .iter()
                    .map(|ix| SimpleInstruction {
                        program_id_index: ix.program_id_index,
                        accounts: ix.accounts.clone(),
                        data: const_hex::encode(&ix.data),
                    })
                    .collect(),
                m.recent_blockhash.to_string(),
            ),
        };

        EnrichedTransaction {
            transaction: SimpleTransaction {
                message: SimpleMessage {
                    account_keys,
                    instructions,
                    recent_blockhash,
                },
                signatures: tx
                    .transaction
                    .signatures
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            meta: TransactionMeta {
                fee: meta.fee,
                pre_balances: meta.pre_balances.clone(),
                post_balances: meta.post_balances.clone(),
                inner_instructions,
                log_messages,
                pre_token_balances: convert_balances(&meta.pre_token_balances),
                post_token_balances: convert_balances(&meta.post_token_balances),
            },
        }
    }
}

impl Plugin for DexSwapPlugin {
    fn name(&self) -> &'static str {
        "dex-swap"
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        tx: &'a TransactionData,
    ) -> PluginFuture<'a> {
        Box::pin(async move {
            if tx.is_vote {
                return Ok(());
            }
            if !Self::is_dex(tx) {
                return Ok(());
            }
            if !Self::has_sol_usdc_usdt(&tx.transaction_status_meta) {
                return Ok(());
            }

            let enriched = Self::enrich(tx);
            self.pending
                .entry(tx.slot)
                .or_insert_with(Vec::new)
                .push(enriched);
            Ok(())
        })
    }

    fn on_block<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        Box::pin(async move {
            let slot = block.slot();
            if slot == 0 {
                return Ok(());
            }
            let prev_slot = slot - 1;

            if let Some((_, txs)) = self.pending.remove(&prev_slot) {
                if txs.is_empty() {
                    return Ok(());
                }
                let (blockhash, block_time, parent_slot) = match block {
                    BlockData::Block {
                        blockhash,
                        block_time,
                        parent_slot,
                        ..
                    } => (blockhash.to_string(), *block_time, *parent_slot),
                    BlockData::PossibleLeaderSkipped { .. } => (String::new(), None, 0),
                };

                let enriched = EnrichedBlock {
                    blockhash,
                    block_time,
                    parent_slot,
                    transactions: txs,
                };

                if let Ok(json) = serde_json::to_string(&enriched) {
                    let path = format!("{}/{}.txt", self.output_dir, prev_slot);
                    let _ = tokio::fs::write(&path, &json).await;
                }
            }
            Ok(())
        })
    }

    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        Box::pin(async move {
            for entry in self.pending.iter() {
                let slot = *entry.key();
                let txs = entry.value();
                if txs.is_empty() {
                    continue;
                }
                let enriched = EnrichedBlock {
                    blockhash: String::new(),
                    block_time: None,
                    parent_slot: 0,
                    transactions: txs.clone(),
                };
                if let Ok(json) = serde_json::to_string(&enriched) {
                    let path = format!("{}/{}.txt", self.output_dir, slot);
                    let _ = tokio::fs::write(&path, &json).await;
                }
            }
            self.pending.clear();
            Ok(())
        })
    }
}
