# DynamoDB-at-Home Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a functional `dynamodb` storage backend to ExtendDB whose data plane forwards to real DynamoDB via `aws-sdk-dynamodb`, while the catalog/IAM/auth plane is delegated to the existing Postgres backend.

**Architecture:** New crate `crates/storage-dynamodb`. A `DynamoEngine` implements the six data-plane traits (`TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`, `BackupEngine`, `WorkerStore`) by translating ExtendDB's internal types/ASTs into DynamoDB SDK calls. The `ServerComponentsRegistration` factory builds `DynamoEngine` for the engine and reuses `PostgresCatalogStore` + `BuiltinAuthProvider` for `catalog_store`/`auth_provider`. Streams and Backups are honest stubs in v1.

**Tech Stack:** Rust, `aws-sdk-dynamodb`, `aws-config`, `inventory` (compile-time registration), `tokio`, `async-trait`, `extenddb-storage`, `extenddb-storage-postgres`, `extenddb-core`, `extenddb-auth`.

**Spec:** `docs/superpowers/specs/2026-06-17-dynamodb-storage-backend-design.md`

---

## Key facts from reconnaissance (do not re-derive)

- **Two `AttributeValue` types.** ExtendDB's internal item is `pub type Item = BTreeMap<String, AttributeValue>` where `AttributeValue` is `extenddb_core`'s own enum (`crates/core/src/types/item.rs`). The SDK has its own `aws_sdk_dynamodb::types::AttributeValue`. The `encoding` module converts between them — this is the real, tested "near-identity" work.
- **All data-plane trait methods return `BoxFuture<'_, Result<…, StorageError>>`**, NOT `async fn`. Use `Box::pin(async move { … })` bodies. (`crates/storage/src/lib.rs`)
- **Conditions are `Expr` ASTs**, key conditions are `KeyCondition`, updates are `&[UpdateAction]`, with names/values in `ExpressionMaps { names: HashMap<String,String>, values: HashMap<String,AttributeValue> }`. (`crates/core/src/expression/`) These must be rendered to DynamoDB expression strings + `ExpressionAttributeNames`/`ExpressionAttributeValues`.
- **Postgres public reuse points** (`crates/storage-postgres/src/lib.rs`, `config.rs`): `PostgresCatalogStore::new(PgPool)`, `PostgresCatalogStore::with_encryption_key(PgPool, String)`, `DbCredentialStore::new(PgPool, String)`, `BuiltinAuthProvider::new(cred_store)`, `parse_connection_string`.
- **Registrations** (mirror `crates/storage-postgres/src/lib.rs`): `BackendRegistration`, `OperationsEngineRegistration`, `StorageConfigRegistration`, `SettingsStoreRegistration`, `DiagnosticsStoreRegistration`, `ServerComponentsRegistration` (factory at postgres lib.rs:407-527; returns `ServerComponents { engine: Arc<dyn StorageEngine>, catalog_store: Arc<dyn CatalogStore>, auth_provider: Arc<dyn AuthProvider>, runtime_hooks: Option<Box<dyn ServerRuntimeHooks>> }`).
- **Build wiring** lives in `crates/bin/Cargo.toml` (`[features]`), `crates/bin/src/main.rs` (`extern crate`), `crates/bin/src/cmd_serve.rs` (feature/backend validation, lines ~70-82, currently postgres-only and must be reworked), `crates/bin/src/cmd_init.rs` (`--backend`), and `extenddb.sample.toml`.
- **`StorageError`** variants of interest for error mapping: `TableNotFound`, `TableAlreadyExists`, `TableNotActive`, `ConditionFailed(Option<Item>)`, `TransactionCanceled(Vec<CancellationReason>)`, `Validation(String)`, `Connection(String)`, `Internal(String)`. (`crates/storage/src/error.rs`)

---

## File structure

```
crates/storage-dynamodb/
  Cargo.toml
  src/
    lib.rs              // module decls + all 6 inventory::submit! registrations + DynamoEngine struct
    config.rs           // DynamoStorageConfig + StorageConfig impl + parse helper
    client.rs           // aws-sdk-dynamodb client construction from config (region/endpoint/creds)
    naming.rs           // account_id + table_name <-> physical DynamoDB table name (athome_ prefix)
    encoding.rs         // core::AttributeValue <-> sdk AttributeValue; Item<->HashMap; key/page tokens
    expression.rs       // Expr/KeyCondition/UpdateAction + ExpressionMaps -> DDB expression strings
    errors.rs           // SdkError / operation errors -> StorageError
    data_engine.rs      // DataEngine impl (forwarding)
    table_engine.rs     // TableEngine impl (forwarding, account-namespaced)
    metadata_engine.rs  // MetadataEngine impl (native TTL, tags, size)
    stream_engine.rs    // StreamEngine impl (honest stubs naming DDB Streams calls)
    backup_engine.rs    // BackupEngine impl (honest stubs naming DDB Backup calls)
    worker_store.rs     // WorkerStore impl (DescribeTable control-plane poll)
    bootstrapper.rs     // Bootstrapper impl (CreateTable provisioning; catalog -> postgres)
    operations.rs       // OperationsEngine impl (delegates catalog_version to postgres semantics)
    server_components.rs// ServerComponentsRegistration factory (the hybrid composition)
  tests/
    integration.rs      // #[ignore]-gated tests against DynamoDB Local + throwaway Postgres
```

Files touched outside the new crate:
- `Cargo.toml` (workspace members + workspace.dependencies: aws-sdk-dynamodb, aws-config)
- `crates/bin/Cargo.toml` (feature + optional dep)
- `crates/bin/src/main.rs` (extern crate)
- `crates/bin/src/cmd_serve.rs` (backend/feature validation rework)
- `crates/bin/src/cmd_init.rs` (help text only; arg already generic)
- `extenddb.sample.toml` (`[storage.dynamodb]` section)
- `docs/differences-from-dynamodb.md` (v1 stub note)

---

## Phasing & PR strategy

Each phase ends green (compiles + tests pass) and is committed. The PR is opened after Phase 9 (compiling crate, unit tests passing, build wired, integration tests written-but-ignored, docs updated). Phases 1-4 are pure unit-testable and carry the real comedic/technical payload; phases 5-8 are SDK forwarding; phase 9 is the hybrid wiring.

> **Commit protocol:** Per repo convention, commit messages are produced by the persona subagent (`~/.claude/commit-persona.md`), not hand-written. Each "Commit" step means: stage the listed files, then dispatch the persona subagent with the staged diff to author the message and commit.

---

## Task 1: Crate skeleton + workspace wiring

**Files:**
- Create: `crates/storage-dynamodb/Cargo.toml`
- Create: `crates/storage-dynamodb/src/lib.rs`
- Modify: `Cargo.toml` (workspace members + workspace.dependencies)

- [ ] **Step 1: Add aws deps + member to workspace `Cargo.toml`**

In `[workspace] members` add `"crates/storage-dynamodb",`. In `[workspace.dependencies]` add under "Internal crates": `extenddb-storage-dynamodb = { path = "crates/storage-dynamodb" }`, and under a new "AWS SDK" group:
```toml
# AWS SDK (DynamoDB-at-home backend)
aws-config = { version = "1", features = ["behavior-version-latest"] }
aws-sdk-dynamodb = "1"
aws-smithy-runtime-api = "1"
```

- [ ] **Step 2: Create `crates/storage-dynamodb/Cargo.toml`**

```toml
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
[package]
name = "extenddb-storage-dynamodb"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
extenddb-core = { workspace = true }
extenddb-storage = { workspace = true }
extenddb-storage-postgres = { workspace = true }
extenddb-auth = { workspace = true }
aws-config = { workspace = true }
aws-sdk-dynamodb = { workspace = true }
aws-smithy-runtime-api = { workspace = true }
async-trait = { workspace = true }
inventory = { workspace = true }
futures = { workspace = true }
sqlx = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
toml = { workspace = true }
tracing = { workspace = true }
bigdecimal = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 3: Create minimal `src/lib.rs` that compiles**

```rust
// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! "DynamoDB at home" storage backend for ExtendDB.
//!
//! The third entry in the satirical-but-functional backend series, and the only
//! one that actually works the way the marketing implies: ExtendDB speaks the
//! DynamoDB wire protocol, and this backend stores its data in *actual*
//! DynamoDB. The point is not the encoding — there is barely any, because
//! DynamoDB is already a key/value database — it is the deployment posture. Run
//! ExtendDB yourself, pointed at DynamoDB, and you are technically "self-hosted."
//! The execs stop asking.
//!
//! Data plane forwards to DynamoDB; the catalog/IAM/auth plane is delegated to
//! the Postgres backend (`extenddb-storage-postgres`), because DynamoDB has
//! opinions about what a database is and "relational IAM catalog" is not one.
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p extenddb-storage-dynamodb`
Expected: success (unused-dep warnings are fine for now).

- [ ] **Step 5: Commit** (persona subagent) — stage `Cargo.toml`, `crates/storage-dynamodb/`.

---

## Task 2: Naming module (account → physical table name)

**Files:**
- Create: `crates/storage-dynamodb/src/naming.rs`
- Modify: `crates/storage-dynamodb/src/lib.rs` (add `pub mod naming;`)

- [ ] **Step 1: Write failing tests** in `naming.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_name_combines_prefix_account_table() {
        let n = Namer::new("athome_");
        assert_eq!(n.physical("123456789012", "Orders"), "athome_123456789012_Orders");
    }

    #[test]
    fn logical_name_round_trips() {
        let n = Namer::new("athome_");
        let phys = n.physical("123456789012", "Orders");
        assert_eq!(n.logical("123456789012", &phys).unwrap(), "Orders");
    }

    #[test]
    fn logical_name_rejects_foreign_account() {
        let n = Namer::new("athome_");
        let phys = n.physical("111111111111", "Orders");
        assert!(n.logical("222222222222", &phys).is_err());
    }

    #[test]
    fn logical_name_preserves_underscores_in_table() {
        let n = Namer::new("athome_");
        let phys = n.physical("123456789012", "my_orders_v2");
        assert_eq!(n.logical("123456789012", &phys).unwrap(), "my_orders_v2");
    }
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p extenddb-storage-dynamodb naming` → FAIL (no `Namer`).

- [ ] **Step 3: Implement `Namer`:**

```rust
//! Maps ExtendDB's (account_id, logical table name) to a flat physical DynamoDB
//! table name. ExtendDB is multi-tenant; DynamoDB tables are flat per AWS
//! account, so we namespace: `<prefix><account_id>_<table>`. Default prefix is
//! `athome_`, because of course it is.

#[derive(Debug, Clone)]
pub struct Namer {
    prefix: String,
}

impl Namer {
    pub fn new(prefix: &str) -> Self {
        Self { prefix: prefix.to_owned() }
    }

    /// `<prefix><account_id>_<table>`
    pub fn physical(&self, account_id: &str, table: &str) -> String {
        format!("{}{}_{}", self.prefix, account_id, table)
    }

    /// Inverse of `physical`, scoped to one account. Errors if `physical` does
    /// not belong to `account_id`.
    pub fn logical(&self, account_id: &str, physical: &str) -> Result<String, String> {
        let want = format!("{}{}_", self.prefix, account_id);
        physical
            .strip_prefix(&want)
            .map(|s| s.to_owned())
            .ok_or_else(|| format!("physical table '{physical}' not in account '{account_id}'"))
    }

    /// The account-scoped prefix used to filter ListTables results.
    pub fn account_prefix(&self, account_id: &str) -> String {
        format!("{}{}_", self.prefix, account_id)
    }
}
```

- [ ] **Step 4: Run tests** — `cargo test -p extenddb-storage-dynamodb naming` → PASS.

- [ ] **Step 5: Commit** (persona subagent).

---

## Task 3: Encoding module (the real, tested near-identity piece)

**Files:**
- Create: `crates/storage-dynamodb/src/encoding.rs`
- Modify: `crates/storage-dynamodb/src/lib.rs` (`pub mod encoding;`)

This converts `extenddb_core` `AttributeValue` ↔ `aws_sdk_dynamodb::types::AttributeValue`, and `Item` (BTreeMap) ↔ `HashMap<String, sdk::AttributeValue>` (the SDK item shape). First confirm the exact shape of `extenddb_core`'s `AttributeValue` enum by reading `crates/core/src/types/item.rs` (variants: S, N, B, Bool, Null, M, L, SS, NS, BS — confirm names/payloads before writing).

- [ ] **Step 1: Read** `crates/core/src/types/item.rs` to get exact `AttributeValue` variant names and payload types (e.g. is binary `Vec<u8>` or `bytes::Bytes`? is `N` a `String`?). Record them.

- [ ] **Step 2: Write failing round-trip tests** covering every variant:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::item::AttributeValue as Core;

    fn round_trip(v: Core) {
        let sdk = to_sdk(&v);
        let back = from_sdk(&sdk);
        assert_eq!(back, v, "round-trip mismatch");
    }

    #[test] fn rt_string()  { round_trip(Core::S("hello".into())); }
    #[test] fn rt_number()  { round_trip(Core::N("123.45".into())); }
    #[test] fn rt_bool()    { round_trip(Core::Bool(true)); }
    #[test] fn rt_null()    { round_trip(Core::Null(true)); }
    #[test] fn rt_binary()  { round_trip(Core::B(vec![0u8, 1, 2, 255])); }
    #[test] fn rt_string_set() { round_trip(Core::SS(vec!["a".into(), "b".into()])); }
    #[test] fn rt_number_set() { round_trip(Core::NS(vec!["1".into(), "2".into()])); }
    #[test] fn rt_binary_set() { round_trip(Core::BS(vec![vec![1,2], vec![3,4]])); }

    #[test]
    fn rt_list_and_map_nested() {
        let mut m = std::collections::BTreeMap::new();
        m.insert("k".to_string(), Core::S("v".into()));
        round_trip(Core::L(vec![Core::N("1".into()), Core::M(m)]));
    }

    #[test]
    fn item_round_trips() {
        let mut item = std::collections::BTreeMap::new();
        item.insert("pk".to_string(), Core::S("u#1".into()));
        item.insert("n".to_string(), Core::N("42".into()));
        let sdk = item_to_sdk(&item);
        assert_eq!(item_from_sdk(sdk), item);
    }
}
```

(Adjust variant constructors to the exact names found in Step 1.)

- [ ] **Step 3: Run to verify failure** — `cargo test -p extenddb-storage-dynamodb encoding` → FAIL.

- [ ] **Step 4: Implement** `to_sdk`, `from_sdk`, `item_to_sdk`, `item_from_sdk`, plus a `key_to_sdk` alias for clarity. Deadpan module doc:

```rust
//! Real, round-trip-tested marshalling between ExtendDB's internal item type and
//! the AWS SDK's. This is, structurally, the identity function: ExtendDB already
//! speaks DynamoDB's type system, so every value maps to its exact namesake.
//! The only reason this file is not empty is that ExtendDB's in-memory Rust enum
//! and the SDK's `AttributeValue` enum are *different Rust types* holding the
//! same data. We translate DynamoDB into DynamoDB. It round-trips because of
//! course it does.
```

Implementation maps each variant 1:1. SDK constructors: `AttributeValue::S(String)`, `::N(String)`, `::Bool(bool)`, `::Null(bool)`, `::B(Blob::new(bytes))`, `::Ss(Vec<String>)`, `::Ns(Vec<String>)`, `::Bs(Vec<Blob>)`, `::L(Vec<AttributeValue>)`, `::M(HashMap<String, AttributeValue>)`. Binary uses `aws_sdk_dynamodb::primitives::Blob`. `item_to_sdk` builds a `HashMap`; `item_from_sdk` collects back into the `BTreeMap` `Item`.

- [ ] **Step 5: Run tests** → PASS. Run `cargo clippy -p extenddb-storage-dynamodb`.

- [ ] **Step 6: Commit** (persona subagent).

---

## Task 4: Error mapping module

**Files:**
- Create: `crates/storage-dynamodb/src/errors.rs`
- Modify: `crates/storage-dynamodb/src/lib.rs` (`pub(crate) mod errors;`)

- [ ] **Step 1: Write failing tests** that construct representative SDK errors and assert the mapped `StorageError` variant. Use the SDK's typed operation-error enums (e.g. `aws_sdk_dynamodb::operation::put_item::PutItemError`) and `SdkError`. Example:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_storage::error::StorageError;

    #[test]
    fn conditional_check_failed_maps_to_condition_failed() {
        let e = aws_sdk_dynamodb::types::error::ConditionalCheckFailedException::builder().build();
        let mapped = map_put_item_error(/* SdkError wrapping e */);
        assert!(matches!(mapped, StorageError::ConditionFailed(_)));
    }
}
```

(Exact constructor wiring is verified against the SDK in Step 3; if direct `SdkError` construction is impractical in a unit test, test the inner classifier function `classify(code: &str) -> StorageError` instead and unit-test that, with one integration check deferred to Task 14.)

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement** a generic `from_sdk_error<E>(err: SdkError<E, R>) -> StorageError` plus a `classify(error_code: &str, message: &str) -> StorageError` helper:
  - `"ConditionalCheckFailedException"` → `StorageError::ConditionFailed(None)`
  - `"ResourceNotFoundException"` → `StorageError::TableNotFound(message)`
  - `"ResourceInUseException"` → `StorageError::TableAlreadyExists(message)`
  - `"TransactionCanceledException"` → `StorageError::TransactionCanceled(vec![])`
  - `"ValidationException"` → `StorageError::Validation(message)`
  - `"ProvisionedThroughputExceededException" | "RequestLimitExceeded" | "ThrottlingException"` → `StorageError::Internal(format!("throttled: {message}"))`
  - dispatch/timeout/IO `SdkError` → `StorageError::Connection(...)`
  - everything else → `StorageError::Internal(...)`
  Use `err.code()` (via `ProvideErrorMetadata`) to extract the code where available.

- [ ] **Step 4: Run tests** → PASS.

- [ ] **Step 5: Commit** (persona subagent).

---

## Task 5: Expression translation module

**Files:**
- Create: `crates/storage-dynamodb/src/expression.rs`
- Modify: `crates/storage-dynamodb/src/lib.rs` (`pub(crate) mod expression;`)

Renders `Expr`, `KeyCondition`, and `&[UpdateAction]` (with `ExpressionMaps`) into DynamoDB expression strings plus `ExpressionAttributeNames` / `ExpressionAttributeValues` maps. Read `crates/core/src/expression/ast.rs`, `key_condition.rs`, `resolver.rs` first for exact variant payloads.

The renderer accumulates a fresh names/values map so the produced expressions are self-contained for the SDK call. Strategy: walk the AST; for each `PathElement::Attribute(a)`, emit `#n0`, `#n1`, … entries; for `Expr::Placeholder(p)`/values, emit `:v0`, `:v1`, … using `ExpressionMaps.values` (converted via `encoding::to_sdk`). `PathElement::Index(i)` renders `[i]`.

- [ ] **Step 1: Write failing unit tests** for each renderer:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::expression::ast::{Expr, CompareOp, PathElement};
    use extenddb_core::expression::resolver::ExpressionMaps;

    #[test]
    fn condition_attribute_exists() {
        let e = Expr::Function { name: "attribute_exists".into(),
            args: vec![Expr::Path(vec![PathElement::Attribute("pk".into())])] };
        let mut r = Renderer::new();
        let s = r.render_condition(&e, &ExpressionMaps::default());
        assert_eq!(s, "attribute_exists(#n0)");
        assert_eq!(r.names().get("#n0"), Some(&"pk".to_string()));
    }

    #[test]
    fn key_condition_pk_eq_and_sk_begins_with() {
        // pk = :v0 AND begins_with(sk, :v1)
        // build KeyCondition with pk_value Placeholder and sk BeginsWith; assert
        // KeyConditionExpression text + names + values.
    }

    #[test]
    fn update_set_and_remove() {
        // SET #n0 = :v0 REMOVE #n1
    }
}
```

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement `Renderer`** with `render_condition(&Expr, &ExpressionMaps) -> String`, `render_key_condition(&KeyCondition, &ExpressionMaps) -> String`, `render_update(&[UpdateAction], &ExpressionMaps) -> String`, and accessors `names() -> &HashMap<String,String>`, `values() -> &HashMap<String, sdk::AttributeValue>`. Handle every `Expr`/`CompareOp`/`ArithOp`/`SortKeyCondition`/`UpdateAction` variant; group `UpdateAction`s into `SET`/`REMOVE`/`ADD`/`DELETE` clauses.

- [ ] **Step 4: Run tests** → PASS.

- [ ] **Step 5: Commit** (persona subagent).

---

## Task 6: Config module + client builder

**Files:**
- Create: `crates/storage-dynamodb/src/config.rs`
- Create: `crates/storage-dynamodb/src/client.rs`
- Modify: `crates/storage-dynamodb/src/lib.rs`

- [ ] **Step 1: Write failing test** for config parse in `config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_full_section() {
        let toml = r#"
            region = "us-east-1"
            endpoint_url = "http://localhost:8000"
            table_prefix = "athome_"
            catalog_connection_string = "postgresql://u:p@localhost/cat"
        "#;
        let t: toml::Table = toml::from_str(toml).unwrap();
        let c = DynamoStorageConfig::from_table(&t).unwrap();
        assert_eq!(c.region, "us-east-1");
        assert_eq!(c.table_prefix, "athome_");
    }

    #[test]
    fn table_prefix_defaults_to_athome() {
        let t: toml::Table = toml::from_str(
            "region=\"us-east-1\"\ncatalog_connection_string=\"postgresql://x\"").unwrap();
        let c = DynamoStorageConfig::from_table(&t).unwrap();
        assert_eq!(c.table_prefix, "athome_");
    }
}
```

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement `DynamoStorageConfig`** (derive `Deserialize, Clone`; `#[serde(default = "default_prefix")]` for `table_prefix` → `"athome_"`; fields: `region: String`, `endpoint_url: Option<String>`, `table_prefix: String`, `catalog_connection_string: String`, optional `pool_size`/`catalog_pool_size` for parity). Implement `extenddb_storage::config::StorageConfig` (`connection_config()` returns the *catalog* connection string so generic catalog plumbing works; `max_connections`/`max_catalog_connections`; `clone_box`). Add `from_table(&toml::Table) -> Result<Self, String>`.

- [ ] **Step 4: Implement `client.rs`** — `async fn build_client(cfg: &DynamoStorageConfig) -> aws_sdk_dynamodb::Client` using `aws_config::defaults(BehaviorVersion::latest()).region(Region::new(cfg.region.clone()))`, applying `.endpoint_url(...)` when set (this is what enables pointing at DynamoDB Local *or another ExtendDB* — the recursion feature).

- [ ] **Step 5: Run tests** → PASS (config); `cargo build` for client.

- [ ] **Step 6: Commit** (persona subagent).

---

## Task 7: DynamoEngine struct + TableEngine impl

**Files:**
- Modify: `crates/storage-dynamodb/src/lib.rs` (define `DynamoEngine { client, namer }`)
- Create: `crates/storage-dynamodb/src/table_engine.rs`

- [ ] **Step 1: Define `DynamoEngine`** in `lib.rs`:

```rust
pub struct DynamoEngine {
    pub(crate) client: aws_sdk_dynamodb::Client,
    pub(crate) namer: crate::naming::Namer,
}

impl DynamoEngine {
    pub async fn from_config(cfg: &crate::config::DynamoStorageConfig) -> Self {
        Self {
            client: crate::client::build_client(cfg).await,
            namer: crate::naming::Namer::new(&cfg.table_prefix),
        }
    }
}
```

- [ ] **Step 2: Implement `TableEngine`** forwarding each method to the SDK with account-namespaced physical names. One fully-worked example (`describe_table`); the rest follow the same shape (translate input → SDK call → map output/error). For `list_tables`, filter SDK results by `namer.account_prefix(account_id)` and strip the prefix back to logical names. For `table_key_info`/`index_info`, build from `DescribeTable` output. Worked example:

```rust
fn describe_table(&self, account_id: &str, input: DescribeTableInput)
    -> BoxFuture<'_, Result<TableDescription, StorageError>> {
    let physical = self.namer.physical(account_id, &input.table_name);
    Box::pin(async move {
        let out = self.client.describe_table().table_name(&physical).send().await
            .map_err(crate::errors::from_sdk_error)?;
        let t = out.table().ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
        Ok(crate::table_engine::to_table_description(t, account_id, &self.namer))
    })
}
```

with a private `to_table_description(&sdk::TableDescription, account_id, &Namer) -> core::TableDescription` mapper (logical name via `namer.logical`, key schema, attribute defs, status, sizes, arn). Read `crates/core/src/types/table.rs` for exact enum mappings (`TableStatus`, `ProvisionedThroughputDescription`, etc.).

- [ ] **Step 3: Build** — `cargo build -p extenddb-storage-dynamodb`. (Behavioral tests are integration, Task 14.)

- [ ] **Step 4: Commit** (persona subagent).

---

## Task 8: DataEngine impl

**Files:** Create `crates/storage-dynamodb/src/data_engine.rs`.

- [ ] **Step 1: Implement `DataEngine`** forwarding all 9 methods:
  - `put_item` → `PutItem` (item via `encoding::item_to_sdk`; condition via `expression::Renderer::render_condition` + names/values; `return_old` → `ReturnValue::AllOld`; map `ConditionalCheckFailedException` → `ConditionFailed`).
  - `get_item` → `GetItem` (key via `item_to_sdk`).
  - `delete_item` → `DeleteItem` (+ condition, return_old).
  - `update_item` → `UpdateItem` (`render_update` + condition; `return_old`/`return_new` → appropriate `ReturnValue`; return `(old, new)`).
  - `query` → `Query` (`render_key_condition`; `scan_index_forward(forward)`; `limit`; `exclusive_start_key` via `item_to_sdk`; `index_name`; return items + `LastEvaluatedKey` as `Option<Item>`).
  - `scan` → `Scan` (`segment`/`total_segments`/`limit`/`index_name`/`exclusive_start_key`).
  - `transact_get_items` → `TransactGetItems` (map each `TransactGetOp`).
  - `transact_write_items` → `TransactWriteItems`, `token` → `.client_request_token(token.1)` for idempotency; map each `TransactWriteOp` variant (Put/Delete/Update/ConditionCheck) to the SDK `TransactWriteItem`.
  - `cleanup_expired_idempotency_tokens` → no-op returning `Ok(0)` with a doc comment: DynamoDB manages its own 10-minute idempotency window; there is nothing to clean.

- [ ] **Step 2: Build** → success.

- [ ] **Step 3: Commit** (persona subagent).

---

## Task 9: MetadataEngine impl

**Files:** Create `crates/storage-dynamodb/src/metadata_engine.rs`.

- [ ] **Step 1: Implement `MetadataEngine`:**
  - `describe_ttl` → `DescribeTimeToLive`; `update_ttl` → `UpdateTimeToLive` (native).
  - `tag_resource`/`untag_resource`/`list_tags` → `TagResource`/`UntagResource`/`ListTagsOfResource`. (DynamoDB tags are keyed by table ARN; resolve the physical table's ARN via `DescribeTable` when the incoming `arn` is an ExtendDB ARN — document the translation.)
  - `refresh_table_size` → no-op (`DescribeTable` is the source of truth; nothing to recompute).
  - `list_active_table_names`/`all_active_tables` → `ListTables` filtered/stripped by account prefix. Cross-account variants (`all_tables_with_ttl`, `all_active_tables`) iterate all physical tables and parse account from the prefix.
  - TTL-index methods (`create_ttl_index`, `drop_ttl_index`, `find_expired_items_indexed`, `tables_with_ttl`, `all_tables_with_ttl_index_ready`) → no-ops / empty results: DynamoDB performs TTL deletion itself, so ExtendDB's TTL worker has nothing to do. Document this.

- [ ] **Step 2: Build** → success. **Step 3: Commit** (persona subagent).

---

## Task 10: StreamEngine + BackupEngine (honest stubs) + WorkerStore

**Files:** Create `stream_engine.rs`, `backup_engine.rs`, `worker_store.rs`.

- [ ] **Step 1: `WorkerStore`** — `process_control_plane_transitions` polls `DescribeTable` for tables this engine created and reports `(logical_name, "ACTIVE")` for any that have reached `ACTIVE`. For v1 simplicity it may return `Ok(vec![])` (DynamoDB tables become ACTIVE on their own and `describe_table` reports real status); document the choice.

- [ ] **Step 2: `StreamEngine` honest stubs** — every method returns `Err(StorageError::Internal(...))` naming the DynamoDB Streams call it maps to, e.g.:

```rust
fn describe_stream(&self, _a: &str, _i: &DescribeStreamInput)
    -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
    Box::pin(async {
        Err(StorageError::Internal(
            "Streams not implemented in the dynamodb backend (v1). Maps to \
             DynamoDB Streams DescribeStream/GetShardIterator/GetRecords.".into()))
    })
}
```

(`assign_shard`/`next_sequence_number`/`write_stream_record` similarly; `cleanup_expired_stream_records` → `Ok(0)`.)

- [ ] **Step 3: `BackupEngine` honest stubs** — each method errors naming the real call (`CreateBackup`, `DescribeBackup`, `ListBackups`, `DeleteBackup`, `RestoreTableFromBackup`, `DescribeContinuousBackups`/`UpdateContinuousBackups`).

- [ ] **Step 4: Build** → `DynamoEngine` now satisfies the `StorageEngine` blanket supertrait. Add a compile assertion in `lib.rs`:

```rust
#[allow(dead_code)]
fn _assert_storage_engine(e: &DynamoEngine) -> &dyn extenddb_storage::StorageEngine { e }
```

- [ ] **Step 5: Commit** (persona subagent).

---

## Task 11: Bootstrapper + OperationsEngine

**Files:** Create `bootstrapper.rs`, `operations.rs`.

- [ ] **Step 1: `Bootstrapper`** (`#[async_trait]`) — holds a `DynamoStorageConfig`. Data-side methods provision via DynamoDB / are no-ops (`create_data_db`, `run_data_migrations`, `record_data_connection` → `Ok(())`; DynamoDB tables are created lazily by `create_table`). Catalog-side methods (`create_catalog_db`, `run_catalog_migrations`, `bootstrap_encryption_key`, `bootstrap_default_account`, `bootstrap_admin_user`, `is_catalog_initialized`, `read_catalog_version`) **delegate to a `PostgresBootstrapper`** built from `catalog_connection_string` (reuse `extenddb_storage_postgres::PostgresBootstrapper`). Sync display methods (`expected_catalog_version`, `catalog_database_name`, `endpoint_info`, `catalog_connection_url`) return DynamoDB-flavored info where it makes sense and delegate catalog naming to postgres. Register the factory in `lib.rs`.

- [ ] **Step 2: `OperationsEngine`** — `parse_connection_string`/`redact_connection_string`/`is_sensitive_key` operate on the catalog (postgres) connection string (reuse `extenddb_storage_postgres::parse_connection_string`); `validate_identifier` validates DynamoDB table-name rules (3-255 chars, `[A-Za-z0-9_.-]`); `catalog_version` mirrors the postgres catalog version. Register a `&'static DynamoOperationsEngine`.

- [ ] **Step 3: Build** → success. **Step 4: Commit** (persona subagent).

---

## Task 12: ServerComponents factory (the hybrid composition) + all registrations

**Files:** Create `server_components.rs`; finalize all `inventory::submit!` blocks in `lib.rs`.

- [ ] **Step 1: Implement the `ServerComponentsRegistration` factory** for `"dynamodb"`, mirroring postgres lib.rs:407-527 but composing across backends:
  1. Downcast `&dyn StorageConfig` to `DynamoStorageConfig` (or re-read the section).
  2. `let engine = Arc::new(DynamoEngine::from_config(cfg).await);`
  3. Build the **catalog pool** from `cfg.catalog_connection_string` (`PgPoolOptions::new().max_connections(cfg.max_catalog_connections()).connect(...)`).
  4. Load encryption key: `SELECT value FROM settings WHERE key = 'encryption_key'`.
  5. `let catalog = Arc::new(PostgresCatalogStore::with_encryption_key(pool.clone(), enc_key.clone()));`
  6. `let cred = DbCredentialStore::new(pool.clone(), enc_key); let auth = Arc::new(BuiltinAuthProvider::new(cred));`
  7. `runtime_hooks`: a minimal `DynamoRuntimeHooks` whose `spawn_workers` starts only the control-plane poller (no TTL worker, no GSI queue); `backend_info()` → `Some("dynamodb (data) + postgres (catalog)".into())`.
  8. Return `ServerComponents { engine, catalog_store: catalog, auth_provider: auth, runtime_hooks: Some(Box::new(hooks)) }`.

- [ ] **Step 2: Add all six `inventory::submit!` blocks** to `lib.rs` (BackendRegistration, OperationsEngineRegistration, StorageConfigRegistration, SettingsStoreRegistration → postgres-backed from `catalog_connection_string`, DiagnosticsStoreRegistration → same, ServerComponentsRegistration). For Settings/Diagnostics factories, the incoming `connection_string` is the catalog (postgres) string, so construct `PostgresCatalogStore::new(PgPool::connect(...))` exactly like postgres does.

- [ ] **Step 3: Build** → `cargo build -p extenddb-storage-dynamodb`. **Step 4: Commit** (persona subagent).

---

## Task 13: Binary build wiring

**Files:** `crates/bin/Cargo.toml`, `crates/bin/src/main.rs`, `crates/bin/src/cmd_serve.rs`, `crates/bin/src/cmd_init.rs`, `extenddb.sample.toml`.

- [ ] **Step 1:** `crates/bin/Cargo.toml` — add to `[features]`: `dynamodb = ["extenddb-storage-dynamodb"]`; add `extenddb-storage-dynamodb = { workspace = true, optional = true }`.

- [ ] **Step 2:** `crates/bin/src/main.rs` — add `#[cfg(feature = "dynamodb")] extern crate extenddb_storage_dynamodb;` next to the existing backend extern crates.

- [ ] **Step 3:** `crates/bin/src/cmd_serve.rs` — **rework the postgres-only validation** (current lines ~70-82) into a set of known-backends checks that accept whichever backends are compiled in. New shape:

```rust
let backend = &app_config.storage._backend;
let mut supported: Vec<&str> = Vec::new();
#[cfg(feature = "postgres")] supported.push("postgres");
#[cfg(feature = "dynamodb")] supported.push("dynamodb");
if !supported.contains(&backend.as_str()) {
    anyhow::bail!("Backend '{}' not enabled in this build. Supported: {}",
        backend, supported.join(", "));
}
```

- [ ] **Step 4:** `crates/bin/src/cmd_init.rs` — update the `--backend` help text to mention `dynamodb` (arg is already generic).

- [ ] **Step 5:** `extenddb.sample.toml` — add commented `[storage.dynamodb]` section:

```toml
# [storage.dynamodb]
# Data plane stores into real DynamoDB; the catalog/IAM plane lives in Postgres.
# region = "us-east-1"
# endpoint_url = "http://localhost:8000"   # DynamoDB Local, or another ExtendDB (recursion is a feature)
# table_prefix = "athome_"
# catalog_connection_string = "postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog"
```

- [ ] **Step 6: Build the binary with the feature** — `cargo build -p extenddb-bin --features dynamodb` and the default build `cargo build -p extenddb-bin`. Both must succeed.

- [ ] **Step 7: Commit** (persona subagent).

---

## Task 14: Integration tests (DynamoDB Local + throwaway Postgres)

**Files:** Create `crates/storage-dynamodb/tests/integration.rs`.

- [ ] **Step 1:** Write `#[ignore]`-gated tests (run only when `DDB_LOCAL_ENDPOINT` and `TEST_CATALOG_URL` env vars are set) that exercise the real round trip: build `DynamoEngine` against DynamoDB Local, `create_table`, `put_item`, `get_item`, `query` with a `begins_with` sort condition, `update_item` with a `SET`, conditional `put_item` that fails with `ConditionFailed`, and `transact_write_items` with an idempotency token. Assert results.

- [ ] **Step 2:** Document in the test file header how to run: `docker run -p 8000:8000 amazon/dynamodb-local`, set env vars, `cargo test -p extenddb-storage-dynamodb -- --ignored`.

- [ ] **Step 3:** Run the **unit** suite to confirm nothing regressed: `cargo test -p extenddb-storage-dynamodb`. If DynamoDB Local is available in the environment, run `--ignored` too and record results.

- [ ] **Step 4: Commit** (persona subagent).

---

## Task 15: Docs + final verification

**Files:** `docs/differences-from-dynamodb.md`, `README.md` (optional mention).

- [ ] **Step 1:** Add a section to `docs/differences-from-dynamodb.md` documenting the `dynamodb` backend: data plane functional, Streams/Backups stubbed in v1, catalog delegated to Postgres, recursion as a feature.

- [ ] **Step 2: Full workspace verification:**
  - `cargo build --workspace` → success
  - `cargo build -p extenddb-bin --features dynamodb` → success
  - `cargo test -p extenddb-storage-dynamodb` → all unit tests PASS
  - `cargo clippy -p extenddb-storage-dynamodb -- -D warnings` → clean
  - `cargo fmt --check` (or run `cargo fmt`)
  - Run pre-commit per repo convention.

- [ ] **Step 3: Commit** (persona subagent), then open the PR.

---

## Self-review

**Spec coverage:**
- Hybrid composition (data→DDB, catalog→Postgres) → Tasks 8, 12. ✓
- Near-identity encoding (real, tested) → Task 3. ✓
- Account-id → physical table namespacing (`athome_`) → Task 2, used in 7-9. ✓
- Data-plane mapping table → Tasks 7-9 (table/data/metadata). ✓
- Condition/key/update expression translation → Task 5, used in 8. ✓
- `transact_write_items` idempotency via `ClientRequestToken` → Task 8. ✓
- TTL native + worker no-op → Task 9. ✓
- Streams/Backups honest stubs → Task 10. ✓
- Error mapping for wire fidelity → Task 4, integration-checked Task 14. ✓
- Config `[storage.dynamodb]` + StorageConfig → Task 6, 13. ✓
- Six inventory registrations → Task 12. ✓
- Build/feature wiring + cmd_serve rework → Task 13. ✓
- Recursion as a feature (endpoint_url) → Task 6 (client), 13 (sample), 15 (docs). ✓
- Testing (unit + DynamoDB Local integration + ExtendDB-on-ExtendDB) → Tasks 3-5 unit, 14 integration. ✓
- Catalog version delegates to postgres → Task 11. ✓

**Placeholder scan:** No "TBD"/"implement later". Two spots intentionally say "read exact variant names/payloads first" (Tasks 3, 5, 7) because the precise `extenddb_core` enum shapes must be confirmed at the file rather than guessed — these are explicit read-then-implement steps, not deferrals.

**Type consistency:** `Namer::physical`/`logical`/`account_prefix` used consistently across Tasks 2/7/9. `encoding::{to_sdk, from_sdk, item_to_sdk, item_from_sdk}` consistent across 3/8. `expression::Renderer::{render_condition, render_key_condition, render_update, names, values}` consistent across 5/8. `DynamoStorageConfig` fields consistent across 6/7/12/13. `from_sdk_error`/`classify` consistent across 4/7/8.

**Known environment risk:** DynamoDB Local may not be available in the execution sandbox; Task 14 tests are `#[ignore]`-gated accordingly, so "ready to PR" = workspace builds + unit tests green + integration tests written. This is called out explicitly rather than hidden.
