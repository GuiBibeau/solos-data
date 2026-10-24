//! The part of a transaction the spot decoders read (ADR-0014): the account keys (static, then
//! loaded through lookup tables, writable first), every instruction in execution order with its
//! stack depth, the log lines, and the token balances (mint, decimals and owner of each token
//! account the transaction names). Built from the archive's native types in the stream, or from
//! the RPC shape (base64 wire and `meta` JSON) for fixtures and tests; both must give the same
//! rows.

use serde_json::Value;

/// A 32-byte account key.
pub type Key = [u8; 32];

/// Base58 of a key.
#[must_use]
pub fn b58(key: &Key) -> String {
    bs58::encode(key).into_string()
}

/// A key from base58, when it is one.
#[must_use]
pub fn key_of(text: &str) -> Option<Key> {
    let mut out = [0u8; 32];
    match bs58::decode(text).onto(&mut out) {
        Ok(32) => Some(out),
        _ => None,
    }
}

/// One instruction, top level or CPI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ix {
    /// Index of the program in [`SpotTx::keys`].
    pub program: u8,
    /// Indexes of its accounts in [`SpotTx::keys`].
    pub accounts: Vec<u8>,
    /// Instruction data.
    pub data: Vec<u8>,
    /// Stack height: 1 at the top level, 2 for a CPI of a top-level instruction, and so on.
    pub depth: u8,
    /// The top-level instruction it belongs to.
    pub top: u16,
}

/// A token account the transaction names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Balance {
    /// Index of the token account in [`SpotTx::keys`].
    pub account: u8,
    /// Its mint, base58 as the metadata gives it.
    pub mint: String,
    /// The mint's decimals.
    pub decimals: u8,
    /// The token account's owner (authority), base58, when recorded.
    pub owner: Option<String>,
    /// Its balance before the transaction (`None` for an account the transaction created).
    pub pre: Option<u64>,
}

/// What the decoders read from one successful transaction.
#[derive(Clone, Debug, Default)]
pub struct SpotTx {
    /// Slot.
    pub slot: u64,
    /// Index of the transaction in its block (the archive's; 0 for RPC fixtures without one).
    pub index: u32,
    /// The fee payer's signature.
    pub signature: Vec<u8>,
    /// Account keys: static, then loaded writable, then loaded read-only.
    pub keys: Vec<Key>,
    /// How many of the first keys signed the transaction.
    pub signers: usize,
    /// Instructions in execution order (a top-level instruction, then its CPIs).
    pub ixs: Vec<Ix>,
    /// Log lines.
    pub logs: Vec<String>,
    /// Token accounts with their mint, decimals and owner (pre balances, then post balances of
    /// accounts the pre balances did not list).
    pub balances: Vec<Balance>,
}

impl SpotTx {
    /// The index of `key` in the account list.
    #[must_use]
    pub fn index_of(&self, key: &Key) -> Option<u8> {
        self.keys
            .iter()
            .position(|k| k == key)
            .and_then(|i| u8::try_from(i).ok())
    }

    /// Whether `key` signed the transaction (a signer is never a program address).
    #[must_use]
    pub fn is_signer(&self, key: &Key) -> bool {
        self.keys.iter().take(self.signers).any(|k| k == key)
    }

    /// The token balance entry of the account at `index`.
    #[must_use]
    pub fn balance(&self, index: u8) -> Option<&Balance> {
        self.balances.iter().find(|b| b.account == index)
    }

    /// Decimals of `mint`, from any token account of that mint the transaction names.
    #[must_use]
    pub fn decimals_of(&self, mint: &Key) -> Option<u8> {
        let text = b58(mint);
        self.balances
            .iter()
            .find(|b| b.mint == text)
            .map(|b| b.decimals)
    }

    /// The signature in base58.
    #[must_use]
    pub fn signature_b58(&self) -> String {
        bs58::encode(&self.signature).into_string()
    }
}

/// Merge pre and post balances: every token account once, its pre entry when it has one (the
/// mint and decimals never change; an account created in the transaction has only a post entry,
/// whose `pre` is `None`, one closed in it only a pre entry).
pub fn merge_balances(pre: Vec<Balance>, post: Vec<Balance>) -> Vec<Balance> {
    let mut out = pre;
    for mut b in post {
        if !out.iter().any(|p| p.account == b.account) {
            b.pre = None;
            out.push(b);
        }
    }
    out
}

fn str_list(meta: &Value, key: &str) -> Vec<Key> {
    meta.get("loadedAddresses")
        .and_then(|l| l.get(key))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter_map(key_of)
        .collect()
}

fn balances_of(meta: &Value, key: &str) -> Vec<Balance> {
    meta.get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|b| {
            Some(Balance {
                account: u8::try_from(b.get("accountIndex")?.as_u64()?).ok()?,
                mint: b.get("mint")?.as_str()?.to_owned(),
                decimals: u8::try_from(b.get("uiTokenAmount")?.get("decimals")?.as_u64()?).ok()?,
                owner: b.get("owner").and_then(Value::as_str).map(str::to_owned),
                pre: b
                    .get("uiTokenAmount")
                    .and_then(|a| a.get("amount"))
                    .and_then(Value::as_str)
                    .and_then(|a| a.parse().ok()),
            })
        })
        .collect()
}

/// A transaction from the RPC shape: the base64 wire and the `meta` object of `getTransaction`
/// (`encoding: base64`), as the fixtures store it.
///
/// # Errors
///
/// A wire that does not parse, or a CPI entry without its program or data.
pub fn from_rpc(tx_b64: &str, meta: &Value, slot: u64, index: u32) -> Result<SpotTx, String> {
    let wire = solana_wire::parse_base64(tx_b64).map_err(|e| e.to_string())?;
    let mut keys = wire.static_account_bytes();
    keys.extend(str_list(meta, "writable"));
    keys.extend(str_list(meta, "readonly"));
    let groups = meta.get("innerInstructions").and_then(Value::as_array);
    let mut ixs = Vec::new();
    for (i, ix) in wire.instructions().iter().enumerate() {
        let top = u16::try_from(i).map_err(|_| "too many instructions")?;
        ixs.push(Ix {
            program: ix.program_id_index,
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
            depth: 1,
            top,
        });
        let inner = groups
            .and_then(|g| {
                g.iter()
                    .find(|g| g.get("index").and_then(Value::as_u64) == Some(i as u64))
            })
            .and_then(|g| g.get("instructions"))
            .and_then(Value::as_array);
        for item in inner.into_iter().flatten() {
            let program = item
                .get("programIdIndex")
                .and_then(Value::as_u64)
                .and_then(|p| u8::try_from(p).ok())
                .ok_or("CPI without a program index")?;
            let data = item
                .get("data")
                .and_then(Value::as_str)
                .and_then(|d| bs58::decode(d).into_vec().ok())
                .ok_or("CPI without base58 data")?;
            let accounts = item
                .get("accounts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|a| a.as_u64().and_then(|a| u8::try_from(a).ok()))
                .collect();
            let depth = item
                .get("stackHeight")
                .and_then(Value::as_u64)
                .and_then(|h| u8::try_from(h).ok())
                .unwrap_or(2);
            ixs.push(Ix {
                program,
                accounts,
                data,
                depth,
                top,
            });
        }
    }
    let logs = meta
        .get("logMessages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    Ok(SpotTx {
        slot,
        index,
        signature: wire.signature_bytes().to_vec(),
        signers: wire.signer_count(),
        keys,
        ixs,
        logs,
        balances: merge_balances(
            balances_of(meta, "preTokenBalances"),
            balances_of(meta, "postTokenBalances"),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_and_reject_other_lengths() {
        let key = [7u8; 32];
        assert_eq!(key_of(&b58(&key)), Some(key));
        assert_eq!(key_of("11"), None);
        assert_eq!(key_of("not base58 0OIl"), None);
    }

    #[test]
    fn post_balances_add_only_accounts_the_pre_balances_lack() {
        let b = |account: u8, mint: &str, pre: Option<u64>| Balance {
            account,
            mint: mint.into(),
            decimals: 6,
            owner: None,
            pre,
        };
        let merged = merge_balances(
            vec![b(1, "A", Some(5))],
            vec![b(1, "A", Some(7)), b(2, "B", Some(3))],
        );
        assert_eq!(merged, vec![b(1, "A", Some(5)), b(2, "B", None)]);
    }
}
