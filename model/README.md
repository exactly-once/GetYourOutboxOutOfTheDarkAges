# TLA+ model of the token-based outbox

`TokenOutbox.tla` is a PlusCal/TLA+ specification of the algorithm described in
[`../docs/token-based-outbox.md`](../docs/token-based-outbox.md). Its structure follows the
[exactly-once workshop model](https://github.com/exactly-once/workshop/tree/master/model-checking).
The PlusCal algorithm is in the comment block, and its TLA+ translation follows it.

## What is modelled

* **Topology:** a chain of logical messages `1 -> 2 -> ... -> ChainLength`. Processing
  message `k` changes business state and emits message `k+1`. A pool of concurrent
  `Handlers` processes all messages, so the same code acts as both receiver
  (it consumes the incoming token) and sender (it creates tokens for its output).
* **Queue** (`queue`, a bag of deliveries): at-least-once. A handler *takes* a delivery,
  and if it crashes before acking, the delivery comes back. A sender can send the same
  message several times, and the `Broker` can deliver up to `MaxDuplicates` extra
  copies of any message at any time, including long after it was processed.
* **Token store** (`tokens`): a set of token ids with create, exists and delete operations.
* **Business database** (`records`, `commits`): per-message transaction records with an
  ETag (`ver`). `commits[k]` counts how many times message `k` changed business state.
  The update in step 4 changes the record and `commits` in one atomic step.
* **Failures:** every write (each label with `either Crash() or ...`) can be followed by a
  crash, up to `MaxFailures` in total. A crashed handler loses its local state, and the
  delivery it held is redelivered. Interleavings between handlers stand in for
  arbitrarily long pauses.

The handler steps map one-to-one to the slides:

| Label | Step |
|-------|------|
| `CreateRecord` | 1. Create record if it doesn't exist |
| `LoadRecord` | 2. Load record (with ETag) |
| `VerifyToken` | 3. Verify incoming token exists, otherwise `DiscardDuplicate` |
| `UpdateRecord` | 4. Update record & store messages + business change, ETag-conditional |
| `CreateTokens` | 5. Create tokens with fresh ids |
| `Checkpoint` | 6. Save token ids in the record, ETag-conditional (on conflict: `ReleaseTokens`) |
| `Send` | 7. Send outgoing message with the checkpointed token |
| `DeleteToken` | 8. Delete incoming token |
| `DeleteRecord` | 9. Delete record |
| `Ack` | acknowledge the delivery |

## Properties

| Name | Kind | Meaning |
|------|------|---------|
| `NoDuplicateProcessing` | invariant | each logical message changes business state at most once |
| `NoGhostMessages` | invariant | a message is sent only after the transaction that produced it committed |
| `ConsistentTokens` | invariant | all copies of a logical message carry the same token |
| `NoLeftoverRecords` | invariant | when the system is idle, no transaction record is left |
| `NoLeftoverTokens` | invariant | when the system is idle, no token of a sent message is left, and orphan tokens ≤ number of crashes |
| `NoLostMessages` | invariant | the token of a sent message is never deleted before the message is processed |
| `NoOrphanTokens` | invariant | when the system is idle, the token store is empty (used to evaluate the `"Register"` extension) |
| `NoOrphanTokensAfterSweep` | invariant | when the system is idle and the sweeper has removed all token-set rows, the token store is empty: no token escaped the sweeper |
| `AllProcessed` | liveness | every message in the chain is eventually processed (no lost messages, no livelock) |
| `OrphansSwept` | liveness | once the system is idle, the sweeper eventually removes all orphan tokens and all token-set rows |

## Design switches

Four boolean constants turn off one design decision each. The `TokenOutbox_*.cfg`
variants turn off exactly one of them to show that the decision is needed.

| Constant | `TRUE` (algorithm) | `FALSE` (broken variant) |
|----------|--------------------|--------------------------|
| `UseCheckpoint` | token ids are persisted before sending | new tokens on every attempt, sent right away |
| `FreshTokenIds` | new token ids on each creation attempt | deterministic ids derived from the outgoing message |
| `UniqueETags` | assumption A1: an update with a stale ETag always fails, even after the record was deleted and re-created | a re-created record starts again at version 0, so a stale ETag can match |
| `LoadBeforeVerify` | load record, then verify token | verify token, then load record |

## Orphan-token cleanup (`OrphanCleanup`)

A fifth constant selects an optional mechanism that removes orphan tokens: tokens that
were created but never checkpointed, and that were left behind by a crash.

| Value | Mechanism |
|-------|-----------|
| `"None"` | base algorithm: orphans can stay, at most one per crash |
| `"Register"` | register ids next to the record, delete registered-but-not-checkpointed tokens at the end |
| `"Sweeper"` | token-set rows that outlive the record, plus a periodic sweeper with a grace period. This is the recommended design. |
| `"SweeperNoGrace"`, `"SweeperRowAfterRecord"`, `"SweeperDropInitialIds"` | `"Sweeper"` with one rule broken each, to show that the rule is needed |

Both extensions share these steps:

| Label | Step |
|-------|------|
| `UpdateRecord` | 4. also generates the first token id and stores it in the same write, so the normal path has no extra round trip. `"Register"` puts it in `registrations`; `"Sweeper"` puts it in the record as `initToken`. |
| `RegisterToken` | 5a. *only when tokens have to be re-generated:* registers a fresh id, but only while the record is processed and not checkpointed. If the record is already checkpointed, goes straight to `Send`. If the record is gone or was re-created, retries. `"Register"` adds the id to `registrations`; `"Sweeper"` inserts a token-set row into `tokenRows`. |

`"Register"` then runs `CleanupRead` and `DeleteOrphans` (8a) before deleting the
record. It can still leave an orphan when a concurrent re-generator creates its token
after that cleanup and then crashes. Registrations are kept outside the record so that
registering doesn't change its ETag. An earlier version kept them in the record, and
TLC found a **livelock**: two handlers re-generating tokens kept invalidating each
other's checkpoints.

`"Sweeper"` instead:

| Label / process | Step |
|-----------------|------|
| `DeleteRecord` | 9. **one transaction:** delete the row of the checkpointed set (it holds live tokens), insert a row for the initial id if it wasn't the one checkpointed, and delete the record |
| `Sweeper` process | for every row whose record no longer exists: delete its token on every pass (always safe), and delete the row only after the grace period D |

The grace period is modelled with a ghost variable `holding`: a row may be deleted
only once the handler that registered its id is no longer in the middle of an
attempt with it. That stands for "D is longer than any attempt". If a real pause
exceeds D, the worst outcome is an orphan token, never a duplicate or a lost message.

## Results

Checked with TLC 2.19 (tla2tools v1.7.4 release jar) and Java 17, using `-deadlock`, since idle handlers
waiting on an empty queue aren't an error.

| Config | Parameters | Result |
|--------|------------|--------|
| `TokenOutbox.cfg` | 2 handlers, chain of 3, 2 crashes, 1 extra delivery, safety + liveness | ✅ no error, 9,881,800 distinct states, ~2.5–4.5 min |
| `TokenOutbox_ThreeHandlers.cfg` | 3 handlers, chain of 2, 2 crashes, 1 extra delivery, safety | ✅ no error, 18,400,250 distinct states, ~1 min |
| `TokenOutbox_NoCheckpoint.cfg` | as main, `UseCheckpoint = FALSE` | ❌ `NoDuplicateProcessing`, 33-step trace: a crash after sending, then the retry creates a new token for message 2 |
| `TokenOutbox_ReusedTokenIds.cfg` | as main, `FreshTokenIds = FALSE` | ❌ `NoDuplicateProcessing`, 34-step trace: a slow handler re-creates token 2 after message 2 was processed |
| `TokenOutbox_ReusedETags.cfg` | as main, `UniqueETags = FALSE` | ❌ `NoDuplicateProcessing`, 20-step trace: ABA, where a stale handler's ETag matches a re-created record |
| `TokenOutbox_VerifyBeforeLoad.cfg` | as main, `LoadBeforeVerify = FALSE` | ❌ `NoDuplicateProcessing`, 21-step trace: a stale handler loads a record that a duplicate re-created |

Orphan tokens with the `"Sweeper"` design:

| Config | Parameters | Result |
|--------|------------|--------|
| `TokenOutbox_Sweeper.cfg` | 2 handlers, chain of 3, 2 crashes, 1 extra delivery, safety incl. `NoLostMessages` and `NoOrphanTokensAfterSweep` | ✅ no error, 42,475,010 distinct states, ~3.5 min |
| `TokenOutbox_Sweeper_Liveness.cfg` | as above but 1 crash, plus `AllProcessed` and `OrphansSwept` | ✅ no error, 3,292,444 distinct states, ~3.5 min |
| `TokenOutbox_SweeperNoGrace.cfg` | rows deleted right away | ❌ `NoOrphanTokensAfterSweep`, 51-step trace: a re-generator creates its token after the sweeper deleted its row, then crashes |
| `TokenOutbox_SweeperRowAfterRecord.cfg` | checkpointed row deleted after the record | ❌ `NoLostMessages`, 20-step trace: between the two deletes the sweeper deletes the **live** token of a sent message. No crash is needed. |
| `TokenOutbox_SweeperDropInitialIds.cfg` | step 9 does not keep non-checkpointed initial ids | ❌ `NoOrphanTokensAfterSweep`, 43-step trace: a late initial token has no row, so the sweeper never finds it |

Orphan tokens with `"None"` and `"Register"`:

| Config | Parameters | Result |
|--------|------------|--------|
| `TokenOutbox_OneHandler.cfg` | 1 handler, `"None"`, `NoOrphanTokens` | ❌ a crash between creating the tokens and the checkpoint leaves an orphan |
| `TokenOutbox_RegisterTokens_OneHandler.cfg` | 1 handler, `"Register"`, `NoOrphanTokens` | ✅ no error, 36,585 distinct states: the token store is always empty when idle |
| `TokenOutbox_RegisterTokens.cfg` | 2 handlers, chain of 3, 2 crashes, 1 extra delivery, `"Register"`, safety | ✅ no error, 17,863,136 distinct states, ~1.5 min |
| `TokenOutbox_RegisterTokens_Liveness.cfg` | as above but 1 crash, safety + liveness | ✅ no error, 1,860,991 distinct states, ~40 s |
| `TokenOutbox_RegisterTokens_Orphans.cfg` | 2 handlers, `"Register"`, `NoOrphanTokens` | ❌ a handler re-generating tokens creates its token after the winner's cleanup ran, and crashes before releasing it. This needs a concurrent copy *and* a crash. |

## Running

You need Java 11+ and [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases).

```bash
JAVA=/path/to/java TLA2TOOLS=/path/to/tla2tools.jar ./check.sh                      # main config
JAVA=/path/to/java TLA2TOOLS=/path/to/tla2tools.jar ./check.sh TokenOutbox_ReusedETags.cfg
```

`check.sh` re-runs the PlusCal translator, so edit only the algorithm in the comment block.
The VS Code TLA+ extension also works: parse the module, then check it with the chosen
`.cfg`, adding `-deadlock` to the TLC options as in the workshop.

## Simplifications

* Each message produces at most one outgoing message, so *send* is atomic. Partial sends
  of several messages behave like "send, crash, resend", which the model covers.
* The token for message 1 is created by an upstream sender that is assumed to be correct.
* The model is bounded: few handlers, a short chain, few crashes and duplicates. TLC
  checks every interleaving within these bounds, which isn't a proof for unbounded
  parameters.
