// Copyright (c) 2022 RBB S.r.l
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

//! Types for ZKThunder L2 batch settlement on Mintlayer.
//!
//! See `docs/ZK_SETTLEMENT_PLAN.md` and `docs/ZK_ENCODING_CONTRACT.md`.

use serialization::{Decode, Encode};

/// The proof system used by the ZKThunder prover for a given protocol version.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Encode,
    Decode,
    serde::Serialize,
    serde::Deserialize,
    strum::EnumIter,
)]
#[repr(u8)]
pub enum ProofType {
    /// The Fflonk proof system (matter-labs `fflonk`), protocol version >= 29
    #[codec(index = 0)]
    Fflonk = 0,
    /// The legacy Plonk proof system (matter-labs `bellman`), protocol version < 29
    #[codec(index = 1)]
    Plonk = 1,
}

/// A ZKThunder verification key, as embedded in the chain config as a consensus constant.
///
/// The bytes are opaque to Mintlayer; only the `zk-verifier` crate interprets them.
///
/// Security invariant: a verification key must *never* be taken from transaction data;
/// it can only come from the chain config (see plan decision D6).
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Encode, Decode, serde::Serialize, serde::Deserialize,
)]
pub struct ZkVerificationKey(Vec<u8>);

impl ZkVerificationKey {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

/// Data carried by a `TxOutput::ZkBatchSettlement` output.
///
/// The four 32-byte hashes below are the public inputs of the ZKThunder SNARK wrapper proof,
/// carried verbatim; see `docs/ZK_ENCODING_CONTRACT.md` for the binding contract between
/// these fields and the proof.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, serde::Serialize, serde::Deserialize)]
pub struct ZkBatchSettlementData {
    /// ZKThunder L2 chain identifier
    pub l2_chain_id: u64,
    /// Monotonically increasing batch number; must be exactly `prev + 1` for this `l2_chain_id`
    pub batch_number: u64,
    /// L2 state root *before* this batch (public input coord; must match the previously settled batch)
    pub prev_state_root: [u8; 32],
    /// L2 state root *after* this batch (public input coord)
    pub state_root: [u8; 32],
    /// Hash of all L2->L1 messages in this batch (public input coord)
    pub l2_to_l1_log_hash: [u8; 32],
    /// Bootloader initial heap contents hash (public input coord)
    pub heap_hash: [u8; 32],
    /// Protocol version of the ZKThunder prover that generated the proof; determines the VK
    pub protocol_version: u32,
    /// The proof system that produced the proof
    pub proof_type: ProofType,
    /// CBOR-serialized `L1BatchProofForL1` as produced by the ZKThunder prover
    pub proof: Vec<u8>,
}

impl rpc_description::HasValueHint for ZkVerificationKey {
    const HINT_SER: rpc_description::ValueHint = rpc_description::ValueHint::HEX_STRING;
}

impl rpc_description::HasValueHint for ProofType {
    const HINT_SER: rpc_description::ValueHint = rpc_description::ValueHint::STRING;
}

impl rpc_description::HasValueHint for ZkBatchSettlementData {
    const HINT_SER: rpc_description::ValueHint = rpc_description::ValueHint::GENERIC_OBJECT;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::chain::{
        ChainConfig, TxOutput, ZkSettlementActivated, config::Builder, config::ChainType,
        transaction::output::TxOutputTag,
    };
    use serialization::{DecodeAll, Encode};

    fn sample_settlement_data() -> ZkBatchSettlementData {
        ZkBatchSettlementData {
            l2_chain_id: 2705,
            batch_number: 42,
            prev_state_root: [1; 32],
            state_root: [2; 32],
            l2_to_l1_log_hash: [3; 32],
            heap_hash: [4; 32],
            protocol_version: 29,
            proof_type: ProofType::Fflonk,
            proof: vec![7; 1000],
        }
    }

    #[test]
    fn zk_batch_settlement_data_roundtrip() {
        let data = sample_settlement_data();
        let encoded = data.encode();
        let decoded = ZkBatchSettlementData::decode_all(&mut encoded.as_slice()).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn zk_batch_settlement_output_codec_index_is_12() {
        let output = TxOutput::ZkBatchSettlement(sample_settlement_data());
        let encoded = output.encode();
        // The variant tag must be exactly 12; a different value would be a
        // consensus-breaking change to the serialization format.
        assert_eq!(encoded[0], 12);
        let decoded = TxOutput::decode_all(&mut encoded.as_slice()).unwrap();
        assert_eq!(output, decoded);
    }

    #[test]
    fn invalid_proof_type_discriminant_is_rejected() {
        let data = sample_settlement_data();
        let encoded = data.encode();

        // Reconstruct the encoded prefix up to (and excluding) the proof_type discriminant
        let mut prefix = Vec::new();
        prefix.extend(data.l2_chain_id.encode());
        prefix.extend(data.batch_number.encode());
        prefix.extend(data.prev_state_root.encode());
        prefix.extend(data.state_root.encode());
        prefix.extend(data.l2_to_l1_log_hash.encode());
        prefix.extend(data.heap_hash.encode());
        prefix.extend(data.protocol_version.encode());
        let discriminant_pos = prefix.len();
        assert_eq!(&encoded[..discriminant_pos], &prefix[..]);
        assert_eq!(encoded[discriminant_pos], 0); // Fflonk discriminant

        let mut corrupted = prefix;
        corrupted.push(99); // no ProofType with discriminant 99
        corrupted.extend_from_slice(&encoded[discriminant_pos + 1..]);
        assert!(ZkBatchSettlementData::decode_all(&mut corrupted.as_slice()).is_err());
    }

    #[test]
    fn output_tag_matches_variant() {
        assert_eq!(
            TxOutputTag::ZkBatchSettlement,
            TxOutput::ZkBatchSettlement(sample_settlement_data()).into(),
        );
    }

    fn chain_config_with_zk(activated: bool) -> Arc<ChainConfig> {
        let mut builder = Builder::new(ChainType::Regtest)
            .consensus_upgrades(crate::chain::upgrades::NetUpgrades::unit_tests());
        if !activated {
            builder = builder.chainstate_upgrades(
                crate::chain::upgrades::NetUpgrades::initialize(vec![(
                    crate::primitives::BlockHeight::new(0),
                    crate::chain::ChainstateUpgradeBuilder::latest()
                        .zk_settlement_activated(ZkSettlementActivated::No)
                        .build(),
                )])
                .unwrap(),
            );
        }
        Arc::new(builder.build())
    }

    #[test]
    fn config_zk_settlement_activated_by_default_on_regtest() {
        let chain_config = chain_config_with_zk(true);
        assert!(chain_config.zk_settlement_activated(crate::primitives::BlockHeight::new(0)));
        assert_eq!(chain_config.zk_batch_settlement_max_proof_size(), 200_000);
        // No VKs registered by default => exact lookup returns None
        assert!(chain_config.zk_vk_for_protocol_version(29, ProofType::Fflonk).is_none());
    }

    #[test]
    fn config_zk_settlement_can_be_disabled() {
        let chain_config = chain_config_with_zk(false);
        assert!(!chain_config.zk_settlement_activated(crate::primitives::BlockHeight::new(0)));
    }
}
