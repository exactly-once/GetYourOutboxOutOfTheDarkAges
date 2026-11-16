# Token-based outbox: deduplication without a growing pile of deduplication data

This document describes the outbox variant from the talk *"Get your outbox out of the
dark ages!"* (`index.html` in this repository). It builds on the ideas in
[Token-based deduplication](https://exactly-once.github.io/posts/token-based-deduplication/)
on exactly-once.github.io, and changes several parts of that design (see
[How this differs from the blog post](#how-this-differs-from-the-blog-post)).

A TLA+ model of the algorithm is in [`../model`](../model/README.md).

---

## 1. The problem

A message handler does three things that can't be put in one transaction:

| Resource       | Characteristics |
|----------------|-----------------|
| Database       | Can't commit the same change twice, **can** check whether something was committed, likely to reject a write (concurrency, constraints) |
| Outgoing queue | **Can't** check whether a message was sent, can send the same message many times, unlikely to reject |
| Incoming queue | Acknowledging is destructive (the message is gone), unlikely to reject |

Messages are delivered *at least once*. Any message can arrive again, and copies can
arrive at the same time or long after the original was handled. The goal is
*exactly-once processing*: each logical message changes business state once, and its
outgoing messages are sent at least once with consistent content.

### The classic outbox and why it's in "the dark ages"

The classic outbox stores, atomically with the business change, a record that
says *"message X was processed and these are the messages it produced"*. A copy
of X that arrives later is recognized as a duplicate because the record exists.

That record has to stay for as long as a copy of X might still arrive, which in
practice means forever. Retention is the weak point:

* deleting data from a database costs a lot and is awkward to do,
* the usual workarounds (deleting in **batches**, rotating **partitions**, a
  **separate store** for deduplication data) only move the garbage around,
* every expiry policy is a bet that no duplicate shows up after the cut-off.

Like a medieval town, the system produces garbage and hopes someone else
will take it away.

## 2. The idea: track what *will* be processed

> ~~track what we processed~~ → track what we **will** process

Instead of remembering processed messages, keep a **token** for every message that
may still be processed:

* The **sender** creates a token for each outgoing message *before* sending it.
  The message carries the token id.
* The **receiver** may process a message only if its token **exists**. When it is
  done, it **deletes** the token. Every later copy finds no token and is dropped as a
  duplicate.

Deduplication data now lives exactly as long as the message is in flight. Nothing
accumulates and nothing needs an expiry policy. The receiver still needs a
per-message **transaction record** (outbox record), but only for the few
moments the message is being processed. The record is deleted at the end.

Sender and receiver must share the token store. In the past this was the hard part,
since nobody wanted a shared database between services. With highly available cloud
key-value stores it is now cheap and practical.

## 3. Building blocks

| Store | Holds | Required operations |
|-------|-------|---------------------|
| **Token store** (shared key-value store) | one entry per in-flight message | create, exists, delete. No transactions and no conditional writes needed. |
| **Business database** | business data + transaction records (one per incoming message being processed) | create-if-not-exists, read with ETag, **conditional** update/replace on ETag that fails for any old ETag ([A1](#a1-a-stale-etag-never-matches)), delete. The record update must be atomic with the business change (same document, partition, or local transaction). |
| **Message queue** | messages | at-least-once delivery. A delivery that isn't acked is redelivered. |

A transaction record contains:

* the incoming message id (key),
* a *processed* marker and the outgoing messages, written together with the business change,
* the ids of the tokens created for the outgoing messages (the *checkpoint*),
* an ETag/version used for optimistic concurrency.

## 4. The algorithm

Every handler is a receiver of its input and a sender of its output.
The final slide of the talk shows the full sequence:

```
 Receive side                                  Send side
 ------------                                  ---------
 1. Create record (if not exists)
 2. Load record  (remember ETag)
 3. Verify incoming token exists ──── no ──▶ duplicate: delete record, ack, stop
 4. Update record & store messages             5. Create tokens (fresh ids)
    (if not yet processed;                     6. Checkpoint: save token ids in record
     conditional on ETag)              ──────▶      (conditional on ETag)
                                               7. Send messages (with checkpointed tokens)
                                               8. Delete incoming token
                                               9. Delete record, ack
```

In pseudo-code:

```text
handle(msg):
    db.CreateIfNotExists(record(msg.Id))                         // 1
    rec = db.Load(msg.Id)                                        // 2  (rec.ETag)
    if not tokenStore.Exists(msg.TokenId):                       // 3
        db.Delete(msg.Id); ack(msg); return                      //    duplicate

    if not rec.Processed:                                        // 4
        (state, outgoing) = businessLogic(msg)
        rec.Processed = true; rec.Outgoing = outgoing
        db.ReplaceIfMatch(rec, businessState, rec.ETag)          //    atomic, OCC
            or retry from the start

    if rec.Outgoing not empty and not rec.Checkpointed:
        ids = NewUniqueIds(rec.Outgoing)                         // 5
        tokenStore.Create(ids)
        rec.TokenIds = ids; rec.Checkpointed = true
        db.ReplaceIfMatch(rec, rec.ETag)                         // 6  checkpoint
            or { tokenStore.Delete(ids); retry from the start }

    for m in rec.Outgoing: send(m with token rec.TokenIds[m])    // 7
    tokenStore.Delete(msg.TokenId)                               // 8
    db.Delete(msg.Id)                                            // 9
    ack(msg)
```

Any step can fail, for example by crashing. The incoming message is then not
acked and comes back, and the handler resumes from what the record says.

## 5. Why every step is where it is

The talk works through the algorithm by running two copies of the same message
side by side.

### Receiver

**A duplicate after the original finished.** *Init record ✅, Verify token ❌.* The
token is gone, so the copy is dropped. The duplicate path deletes the record it just
(re-)created, so no garbage remains.

**Concurrent duplicates (naive version: init, verify, insert, delete token, delete record).**
Both copies pass *Verify token*. The first *Insert record* wins, and the second
fails on the uniqueness constraint. It looks fine.

**Concurrent duplicates with a long pause: disaster.** Copy B pauses after verifying
the token. Copy A finishes everything, including *Delete record*. B then wakes up and
its *Insert record* succeeds, because the record is gone, so the message is
processed twice. Uniqueness of the record isn't enough: its whole lifecycle must
be protected.

**Fix: optimistic concurrency on the record.** Create the record first (no matter which copy
wins), *load* it, verify the token, and finish with an *update* that succeeds only if
the record still has the ETag that was loaded. In the scenario above, B's update now
fails because the record was updated and deleted in the meantime.

The model checker finds two more conditions that this fix depends on:

* **Load before verify.** If the token is verified *before* the record is loaded,
  copy A can finish and delete the record, and a third copy can re-create a fresh
  record in between. B then loads that fresh, unprocessed record with a valid ETag
  and processes the message again. Because B loaded first, any record it sees
  was there *before* the token disappeared.
* **An update with an old ETag must fail, even if the record was deleted and
  re-created in the meantime** (see [Assumption A1](#a1-a-stale-etag-never-matches)).
  The token check and the conditional update are separate operations. The ETag is
  the only thing that tells B its token was valid for the record it is about to
  commit. If a re-created record could get B's old ETag back, B's update would
  succeed and the message would be processed twice. This is the ABA problem. The
  model assumes A1, and a variant without it shows that it's needed.

### Sender

Tokens must be created **before** the messages are sent, because the receiver
must never see a message without a token. They must be created **at most once**
for a given set of outgoing messages, because re-creating a token after the receiver
has consumed it would let a duplicate through.

**Without a checkpoint:** *Store messages, Create tokens, Send message 1*. The receiver
processes message 1 and deletes its token. *Send message 2 ❌.* On retry the sender
creates *new* tokens and sends message 1 again, and that copy is processed a second time.

**Checkpoint:** after creating the tokens, their ids are stored in the record
(conditionally on the ETag). From then on every retry sends the outgoing messages
with exactly these tokens and never creates new ones.

* *Create tokens ✅, Checkpoint ❌.* The retry creates another set of tokens and
  checkpoints those. The first set is never referenced by any message, so it is
  harmless (see garbage below).
* *Checkpoint ✅, Send message 1 ✅, Send message 2 ❌.* The retry sends both
  messages again with the same tokens. The receiver drops the extra copy of message 1.

**Token ids must be fresh on every attempt.** Using ids derived from the
outgoing message, so that the same ids are used each time, looks attractive
because it avoids garbage tokens. It is unsafe. A slow copy that loaded the
record before the checkpoint can *re-create* a token that the receiver already
consumed and deleted, and the next copy of that message is processed again. The
model checker finds this.

### Cleanup

The incoming token is deleted only after all outgoing messages have been sent
(step 8). From that moment every copy of the incoming message is a duplicate.
The record is deleted last (step 9). Deleting it never breaks a
concurrent copy: that copy's ETag-conditional write fails, and when it retries
it finds no token.

## 6. What garbage is left

* **Transaction records:** none. Every record is deleted either by the copy that
  processed the message or by a duplicate that finds no token.
* **Tokens of sent messages:** none. They are deleted by the receiver.
* **Orphan tokens:** tokens created in step 5 whose checkpoint did not succeed.
  * If the checkpoint is *rejected* (ETag conflict), the handler knows those ids
    were never published and deletes them right away. This is a small refinement
    over the talk, and it is part of the model.
  * If the handler *crashes* between step 5 and a successful checkpoint, the tokens
    stay. This needs a crash in a short window, and each such crash leaves at most
    one set of tokens. A token that no message references can never let a
    duplicate through, so this costs storage only and never affects correctness.
    The model verifies that, once the system is idle, the number of tokens left is
    no more than the number of crashes.

### Optional extension: token-set rows and a sweeper (recommended)

The orphans above are rare, but without extra machinery they are never removed. No
cleanup that runs *inside* the protocol can guarantee to remove them: an orphan comes
from a token-store write by a handler that is either slow or has crashed, and in an
asynchronous system those two can't be told apart. The fix therefore uses a time
bound, but only for completeness, never for safety.

Deleting a token that was never checkpointed is **always safe**: no message carries
it, so deleting it can't cause duplicate processing or a lost message, however late
the delete happens. The design builds on that:

* **Step 4** also generates the id of the first token and stores it in the record,
  in the same atomic write. The happy path costs no more than the base algorithm.
* **Step 5a, only when tokens have to be re-generated** (the record is processed but
  not checkpointed, and this attempt has no id yet): insert a **token-set row**
  `(MessageId, TokenId)` linked to the record, then create the token. The insert runs
  as one operation together with the check that the record is processed and not
  checkpointed (`INSERT ... SELECT ... WHERE EXISTS`, or a transactional batch).
  If the record is already checkpointed, send with the checkpointed tokens. If it is
  gone or was re-created, give up and retry the message.
* **Step 9, one transaction:** delete the row of the checkpointed set, if there is
  one; insert a token-set row for the initial ids if they are not the checkpointed
  set; delete the record. On the happy path the initial ids *are* the checkpointed
  set, so this is just the record delete.
* **A periodic sweeper**, for every token-set row whose record no longer exists:
  * deletes the row's tokens on every pass;
  * deletes the row itself only once it is older than a grace period D, for example
    a day, longer than any realistic pause of a handler.

Rules, each of which the model breaks on purpose to show that it is needed:

1. **The checkpointed set's row goes before the record, or in the same transaction.**
   Otherwise the sweeper can see a row of *live* tokens with no record and delete
   them. The outgoing messages are then dropped as duplicates and lost. The model
   finds this without any crash, in the moment between the two deletes.
2. **Initial ids that were not checkpointed become a row when the record is deleted.**
   Otherwise a handler that pauses after step 4, creates its initial token late and
   crashes leaves a token that no row refers to.
3. **Rows are deleted only after the grace period.** Without it, a re-generator that
   creates its token after its row was swept, and then crashes, leaves an orphan.
4. **Token-set rows never change the record's ETag.** Keep them as separate rows or
   documents (a child table in SQL, separate documents in the same Cosmos DB
   partition). An earlier variant that wrote ids into the record itself livelocked:
   two re-generating handlers kept invalidating each other's checkpoints.

What it achieves (model-checked, with D modelled as "longer than any attempt"):

* Safety and liveness are unchanged, including the new check that the token of a
  sent message is never deleted before the message is processed.
* **Once the system is idle, the sweeper removes every orphan token and every row.**
  Garbage is limited to messages in flight plus rows younger than D.
* If a handler pauses for longer than D, the worst case is an orphan token, never a
  duplicate or a lost message.

A simpler variant without the sweeper (delete registered-but-not-checkpointed tokens
in step 8a, then forget them) removes orphans when only one copy of a message is in
flight, but leaves one case forever: a concurrent re-generator that creates its token
after that cleanup and crashes. The model keeps both variants (`OrphanCleanup =
"Register"` and `"Sweeper"`).

## 7. How this differs from the blog post

The [token-based deduplication post](https://exactly-once.github.io/posts/token-based-deduplication/)
describes an earlier version of the same idea. The main differences:

| | Blog post | This talk |
|---|---|---|
| Where the transaction state lives | inside the business entity (`entity.OutboxState[messageId]`), protected by the entity's version | a dedicated per-message transaction record with an explicit lifecycle: create, load, update, delete |
| Duplicate detection | token check, then entity version check on persist | token check, then ETag check on the record update. The record is loaded before the token check. |
| Orphan tokens from failed attempts | removed with a separate *register-cleanup* step, which adds one more round trip to **every** message | accepted as rare, bounded garbage that can't cause duplicates, so the normal path has no extra round trip. A rejected checkpoint releases its tokens right away (a refinement in the model). The optional sweeper extension (section 6) removes all of them, given a grace period, and writes extra rows only on the rare re-generation path, not on every message. |
| Ordering subtleties made explicit | - | load the record before verifying the token; a stale ETag never matches, even after re-creation (A1); fresh token ids on every attempt (all three confirmed necessary by the model) |

## 8. Requirements and assumptions

### A1. A stale ETag never matches

A conditional update (steps 4 and 6) with an ETag that was loaded earlier
**fails if the record changed in any way since then**, and that includes the
record being deleted, whether or not it was re-created afterwards. In other
words, an ETag identifies one version of one *lifetime* of the record, and it is
never handed out again.

How this looks in practice:

* **SQL Server:** add a `rowversion` column, and run the update as
  `UPDATE OutboxRecord SET ... WHERE MessageId = @id AND Version = @loadedVersion`.
  Treat the update as successful only if `@@ROWCOUNT = 1`. If the row was deleted,
  no row matches. If it was re-created, the new row has a new `rowversion`, because
  `rowversion` values grow across the whole database and are never reused. Either way
  0 rows are updated and the attempt is retried.
* **Cosmos DB / Azure Table and Blob storage:** replace with an `If-Match: <etag>`
  condition. The store generates the ETags and never reuses them.
* **Not sufficient:** a version number that the application stores and increments
  itself, starting at 0 or 1 whenever the record is created. After a delete and a
  re-create, an old version number can match again. The model variant
  `TokenOutbox_ReusedETags.cfg` shows how this leads to duplicate processing.

### Other assumptions

* The token store is linearizable for single-key operations: after a delete, an
  `Exists` check returns `false`.
* The record update in step 4 and the business data change are atomic.
* Token ids are newly generated (for example as GUIDs) on every token-creation attempt.
* The queue delivers at least once. A message is acked only after step 9 or after it is
  recognized as a duplicate.
* There is no expiry on tokens. A token must exist for as long as its message can still
  be delivered.
