# ZKThunder Batch Settlement — Implementation Plan

Implementation of [`zk-thunder/docs/MINTLAYER_UPGRADE.md`](https://github.com/erubboli/zk-thunder/blob/main/docs/MINTLAYER_UPGRADE.md):
native ZK proof verification for ZKThunder L2 batch settlement on Mintlayer.

Companion document: [`ZK_ENCODING_CONTRACT.md`](./ZK_ENCODING_CONTRACT.md) — the byte-level
public-input encoding specification. **This document is the external audit artifact and must
be signed off before the verifier code in P2 is written against it.**

## Status

| Phase | Scope | Status |
|-------|-------|--------|
| P0 | Encoding contract written + externally audited | Draft written, audit pending |
| P1 | Output type, upgrade gating, config, match-arm sweep | In progress |
| P2 | `zk-verifier` crate (fail-closed skeleton → real crypto) | Pending P0 signoff |
| P3 | tx-verifier integration (stateless + sequencing + undo/reorg) | Pending P1 |
| P4 | RPC + tests + docs | Pending |

## PR split

1. **PR 1 (P1):** `ZkBatchSettlement` output variant + `ZkSettlementActivated` upgrade flag +
   config methods + compiler-driven sweep of all exhaustive matches. No-op until activated:
   outputs of the new type are rejected when the flag is off (unknown at old codec index;
   rejected by the activated-gate at new nodes), so this is safe to merge ahead of activation.
2. **PR 2 (P2+P3):** `zk-verifier` crate, verification pipeline, batch sequencing state,
   per-block undo, persistence, tests.
3. **PR 3 (P4):** RPC surface, docs, activation heights for mainnet/testnet.

## Design decisions (with rationale)

### D1 — VerificationKey lives in `common`, not `zk-verifier`
`ChainConfig` (in `common`) must return `Option<&VerificationKey>`; `common` cannot depend on
`chainstate` crates. The VK type is therefore a small opaque newtype in
`common/src/chain/zk.rs`; the `zk-verifier` crate consumes it.

### D2 — Activation via existing `ChainstateUpgrade` mechanism
Add `zk_settlement_activated: ZkSettlementActivated` (Yes/No) to `ChainstateUpgrade`, following
the exact `HtlcActivated` pattern: enum + builder method + every `NetUpgrades<ChainstateUpgrade>`
table in `common/src/chain/config/builder.rs` and `chainstate_upgrades_builder.rs`.
All zk validation paths check `chainstate_upgrades().version_at_height(height).1.zk_settlement_activated()`.

### D3 — Sequencing state needs per-block undo, not a single "latest" key
The upstream spec's storage sketch (`zk_settled ++ chain_id -> latest`) is **incorrect under
forks**: two competing blocks at height H can each contain a settlement for batch N, and both
would pass the `prev+1` check against the same parent state. Follow the established accounting
pattern instead:

- Schema: `DBZkSettledBatch: Map<u64 /* l2_chain_id */, ZkSettledBatch>` where
  `ZkSettledBatch { batch_number: u64, state_root: [u8; 32] }`
- Schema: `DBZkSettlementBlockUndo: Map<Id<Block>, BlockUndo<ZkSettlementUndo>>` where
  `ZkSettlementUndo` records the **previous** value (or `None`) per chain id touched by the block.
- `disconnect_block` restores the previous value from undo, exactly like
  `DBOrdersAccountingBlockUndo`.

### D4 — `proof_type` vs CBOR tag: CBOR tag is authoritative
`proof_type` is an advisory fast-reject hint only (spec agrees). The deserializer dispatches on
the CBOR structure itself; if the discriminant disagrees with the deserialized variant, reject.

### D5 — Public inputs: 4 coords, all must be bound
The upstream spec's `build_public_inputs(state_root, l2_to_l1_log_hash) -> [[u8;32];4]` is
underspecified and unsound as written. `ZkBatchSettlementData` carries **all four** public-input
coords explicitly (prev_state_root, state_root, l2_to_l1_log_hash, heap_hash); the verifier
reconstructs the exact coord array and the tx-verifier additionally enforces
`prev_state_root == settled state of batch N-1` as a stateful check. Byte-level details are the
subject of the encoding contract doc.

### D6 — VK discipline (security invariants)
- VK originates **only** from `ChainConfig` consensus constants; never parsed from tx bytes.
- Exact `(protocol_version, proof_type)` lookup; no fallback-to-latest (downgrade protection).
- Node startup parses and sanity-checks all embedded VKs once (fail fast at boot, not at tx time).

### D7 — Fail-closed deserializer
The `zk-verifier` crate denies `unwrap`/`expect`/panics on untrusted input
(`#![deny(clippy::unwrap_used, clippy::expect_used)]` where feasible), and gets a `cargo-fuzz`
target over `parse + verify` before merge.

## Risk register

| Risk | Severity | Mitigation |
|------|----------|------------|
| Public-input encoding mismatch → false settlement | Critical | Encoding contract doc + external audit (P0) + coord-swap/bit-flip known-answer tests + cross-check vs zkSync Solidity verifier |
| Unbound prev-state (2-vs-4 gap) → forged transitions | Critical | D5: all four coords carried and verified; prev-root continuity checked statefully |
| VK mishandling | High | D6 invariants |
| Panic on attacker CBOR → consensus DoS | High | D7 + fuzzing |
| matter-labs git deps: supply chain, wasm build | Medium | `cargo vet`/`deny.toml` pass; early wasm32 compile check of `zk-verifier` in isolation |
| 200 KB proofs vs tx-size/fee limits | Medium | P1 audits `max_tx_size`, block size, and mempool fee floor; sized like DataDeposit fee policy |
| Verification latency (~100–500 ms/settlement) | Low | Bounded by sequencing: ≤1 settlement/block expected; document for block producers |

## File map

| File | Change |
|------|--------|
| `common/src/chain/zk.rs` | **New** — `ZkBatchSettlementData`, `ZkVerificationKey`, `ProofType` |
| `common/src/chain/transaction/output/mod.rs` | `ZkBatchSettlement` variant (codec index 12) |
| `common/src/chain/upgrades/chainstate_upgrade/*` | `ZkSettlementActivated` flag |
| `common/src/chain/config/*` | `zk_batch_settlement_max_proof_size`, `zk_vk_for_protocol_version`, `zk_settlement_activated_at` |
| `chainstate/zk-verifier/` | **New crate** — `verify()`, CBOR parsing, fuzz target |
| `chainstate/storage/src/schema.rs` | `DBZkSettledBatch`, `DBZkSettlementBlockUndo` |
| `chainstate/tx-verifier/src/transaction_verifier/check_transaction.rs` | Size guard + VK lookup + verify |
| `chainstate/tx-verifier/src/transaction_verifier/input_output_policy/*` | Non-spendable, one-per-tx, not-in-reward arms |
| `chainstate/tx-verifier/src/transaction_verifier/mod.rs` | Sequencing check + undo write in `connect_transaction` |
| `rpc/` | `getlatestsettledbatch` |

## Testing requirements (from upstream spec, extended)

1. **zk-verifier unit**: known-good Fflonk proof passes; per-coord bit-flip fails; per-coord
   *swap* fails (ordering bug canary); malformed CBOR fails; truncated proof fails.
2. **tx-verifier integration**: valid settlement connects; invalid proof rejects block; batch
   gap rejects; duplicate batch rejects; wrong `prev_state_root` rejects.
3. **Persistence**: latest settled batch survives restart.
4. **Reorg**: disconnecting a settlement block restores the previous batch counter (D3 undo).
5. **Gating**: settlement output in a block *before* activation height is rejected.
6. **Fuzz**: `cargo-fuzz` target over parse+verify; no panics reachable from tx bytes.

## Open questions

1. Real VK bytes + prover-generated proof fixtures from the zkthunder team (needed before P2
   can ship known-answer tests; P2 skeleton proceeds with locally generated fixtures).
2. Activation heights per network (end of PR 3).
3. Single `l2_chain_id` restriction at launch vs multi-chain from day one (spec supports both;
   recommend multi-chain from day one — the state is keyed, no extra cost).
4. Fee model: size-proportional (DataDeposit-style) vs flat surcharge. Recommend
   size-proportional + modest flat component for verification cost.
