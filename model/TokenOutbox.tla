---------------------------- MODULE TokenOutbox ----------------------------
(***************************************************************************)
(* Token-based outbox ("Get your outbox out of the dark ages!").            *)
(*                                                                         *)
(* Instead of remembering which messages were already processed, the       *)
(* system keeps a TOKEN for every message that still MAY be processed. A    *)
(* message is processed only if its token is present. Processing ends by    *)
(* deleting the token, so the deduplication data lives only as long as the  *)
(* message is in flight.                                                    *)
(*                                                                         *)
(* Topology modelled here: a causal chain of logical messages              *)
(*     1 -> 2 -> ... -> ChainLength                                        *)
(* Processing message k changes business state and emits message k+1 (the  *)
(* last one emits nothing). Every message is processed by the same pool of *)
(* concurrent Handlers, so each handler is at the same time a RECEIVER     *)
(* (consumes a token) and a SENDER (creates tokens for its output).        *)
(*                                                                         *)
(* Stores:                                                                  *)
(*   queue    - at-least-once message queue: a bag of deliveries. A handler *)
(*              takes a delivery; if it crashes before acking, the delivery *)
(*              becomes visible again. Senders may send the same message    *)
(*              many times and the Broker may duplicate any message, so     *)
(*              several handlers can work on copies of one message at once. *)
(*   tokens   - the shared token store (key-value store, no transactions)  *)
(*   records  - per-incoming-message transaction (outbox) records, stored   *)
(*              in the business database; updates use optimistic           *)
(*              concurrency on an ETag (ver)                               *)
(*   commits  - business state: how many times message k changed the data  *)
(*                                                                         *)
(* No two of these stores share a transaction. Every write can be followed *)
(* by a crash (bounded by MaxFailures) after which the delivery is retried. *)
(*                                                                         *)
(* The four boolean constants switch individual design decisions of the   *)
(* algorithm off, to show that each of them is necessary. OrphanCleanup    *)
(* selects an optional mechanism for removing orphan tokens.               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Bags

CONSTANTS
    Handlers,         \* ids of concurrent handler instances (positive integers, not 99)
    ChainLength,      \* number of logical messages in the causal chain
    MaxFailures,      \* total number of crashes allowed in a behaviour
    MaxDuplicates,    \* how many extra copies of messages the broker may deliver
    UseCheckpoint,    \* persist outgoing token ids before sending
    FreshTokenIds,    \* every token-creation attempt uses brand new token ids
    UniqueETags,      \* assumption A1: a stale ETag never matches, even after delete/re-create
    LoadBeforeVerify, \* load the record BEFORE checking the incoming token
    OrphanCleanup     \* "None"     - base algorithm
                      \* "Register" - register ids next to the record, delete
                      \*              registered-but-not-checkpointed tokens at the end
                      \* "Sweeper"  - token-set rows that outlive the record, removed
                      \*              by a periodic sweeper after a grace period
                      \* "SweeperNoGrace", "SweeperRowAfterRecord",
                      \* "SweeperDropInitialIds" - Sweeper with one rule broken

MsgIds       == 1..ChainLength
Out(k)       == IF k < ChainLength THEN {k + 1} ELSE {}
InitialToken == 0
BrokerId     == 0
SweeperId    == 99
One(m)       == SetToBag({m})

Register   == OrphanCleanup = "Register"
UseSweeper == OrphanCleanup \in {"Sweeper", "SweeperNoGrace", "SweeperRowAfterRecord",
                                 "SweeperDropInitialIds"}
\* Both extensions generate the first token id in step 4 and register re-generated ids.
Registers  == Register \/ UseSweeper

\* "No record" marker. All records share one shape to keep TLC comparisons simple.
\* initToken: (Sweeper) id of the first token, stored in the record by step 4.
NoRecord == [exists |-> FALSE, ver |-> 0, processed |-> FALSE,
             checkpointed |-> FALSE, token |-> 0, initToken |-> 0]

\* (Register) registered token ids that a cleanup of record r may delete.
Orphans(regs, r) == regs \ (IF r.checkpointed THEN {r.token} ELSE {})

(*--algorithm TokenOutbox

variables
    queue      = One([id |-> 1, token |-> InitialToken]),
    sent       = {[id |-> 1, token |-> InitialToken]},  \* everything ever put on the wire
    tokens     = {InitialToken},                         \* token of message 1, created by an (assumed correct) upstream
    records    = [k \in MsgIds |-> NoRecord],
    \* (Register) token ids registered for record k. Kept in separate rows/documents
    \* next to the record, so registering does NOT change the record's ETag.
    registrations = [k \in MsgIds |-> {}],
    \* (Sweeper) token-set rows [tok, msg]: separate rows/documents linked to record msg.
    \* They do not change the record's ETag and they outlive the record.
    tokenRows  = {},
    \* Ghost (not part of the algorithm): the registered token id that handler h may
    \* still create in its current attempt. Used only to model the sweeper's grace period.
    holding    = [h \in Handlers |-> 0],
    commits    = [k \in MsgIds |-> 0],
    nextVer    = 1,                                      \* source of unique ETags
    nextToken  = ChainLength + 1,                        \* source of fresh token ids
    failures   = 0,
    duplicates = 0;

define
    NewVer(r)  == IF UniqueETags THEN nextVer ELSE r.ver + 1
    SentTokens == {m.token : m \in sent}
    Quiescent  == BagToSet(queue) = {} /\ \A h \in Handlers : pc[h] = "MainLoop"

    \* (Sweeper) rows to change when record r of message k is deleted (step 9):
    \* the row of the checkpointed set goes away (it holds live tokens) ...
    CheckpointedRows(r, k) ==
        IF r.checkpointed THEN {x \in tokenRows : x.msg = k /\ x.tok = r.token} ELSE {}
    \* ... and initial ids that were not checkpointed become a row for the sweeper.
    InitialRows(r, k) ==
        IF /\ r.initToken # 0
           /\ ~(r.checkpointed /\ r.token = r.initToken)
           /\ OrphanCleanup # "SweeperDropInitialIds"
        THEN {[tok |-> r.initToken, msg |-> k]} ELSE {}

    TypeOK ==
        /\ BagToSet(queue) \subseteq sent
        /\ \A m \in sent : m.id \in MsgIds /\ m.token \in Nat
        /\ tokens \subseteq Nat
        /\ tokenRows \subseteq [tok : Nat, msg : MsgIds]
        /\ \A k \in MsgIds : commits[k] \in Nat

    \* SAFETY: business state is changed at most once per logical message.
    NoDuplicateProcessing == \A k \in MsgIds : commits[k] <= 1

    \* SAFETY: a message is only emitted by a transaction that was committed.
    NoGhostMessages == \A m \in sent : m.id > 1 => commits[m.id - 1] >= 1

    \* SAFETY: every copy of a logical message carries the same token.
    ConsistentTokens == \A m1, m2 \in sent : m1.id = m2.id => m1.token = m2.token

    \* SAFETY: the token of a sent message is not deleted before the message is
    \* processed (otherwise the message would be dropped as a duplicate - lost).
    NoLostMessages == \A m \in sent : commits[m.id] = 0 => m.token \in tokens

    \* SPACE: once nothing is in flight, no transaction record is left behind ...
    NoLeftoverRecords == Quiescent => \A k \in MsgIds : ~records[k].exists

    \* ... no token of a sent message is left behind, and the only garbage tokens are
    \* the ones abandoned by a crash between "create tokens" and "checkpoint".
    NoLeftoverTokens == Quiescent => /\ tokens \cap SentTokens = {}
                                     /\ Cardinality(tokens) <= failures

    \* SPACE (stronger): once nothing is in flight, the token store is empty.
    NoOrphanTokens == Quiescent => tokens = {}

    \* SPACE (Sweeper): once nothing is in flight and the sweeper has removed all
    \* rows, the token store is empty - no token escaped the sweeper.
    NoOrphanTokensAfterSweep == (Quiescent /\ tokenRows = {}) => tokens = {}

    \* LIVENESS: every message in the chain is eventually processed.
    AllProcessed == <>(\A k \in MsgIds : commits[k] >= 1)

    \* LIVENESS (Sweeper): once nothing is in flight, the sweeper eventually
    \* removes all orphan tokens and all rows.
    OrphansSwept == Quiescent ~> (tokens = {} /\ tokenRows = {})
end define;

\* A crash of the handler. The delivery it was working on was never acked,
\* so the broker makes it visible again (lease/lock expiry).
macro Crash() begin
    await failures < MaxFailures;
    failures := failures + 1;
    queue := queue (+) One(msg);
    goto MainLoop;
end macro;

\* Abandon this attempt (e.g. optimistic concurrency conflict) and let the delivery
\* be retried. Not counted as a failure: it only happens when someone else made progress.
macro Retry() begin
    queue := queue (+) One(msg);
    goto MainLoop;
end macro;

fair process Handler \in Handlers
variables
    msg     = [id |-> 1, token |-> InitialToken],
    rec     = NoRecord,   \* local copy of the transaction record (incl. ETag)
    myToken = 0,          \* token created for the outgoing message in this attempt
    regs    = {};         \* (Register) registrations read during cleanup
begin
MainLoop:
    await BagToSet(queue) # {};
    with m \in BagToSet(queue) do
        msg := m;
        queue := queue (-) One(m);   \* take one delivery
    end with;
    rec := NoRecord;
    myToken := 0;
    regs := {};
    holding[self] := 0;

\* 1. Create record (if not exists). Does not matter which concurrent thread wins.
CreateRecord:
    either Crash();
    or
        if ~records[msg.id].exists then
            records[msg.id] := [NoRecord EXCEPT !.exists = TRUE,
                                                !.ver = IF UniqueETags THEN nextVer ELSE 0];
            nextVer := nextVer + 1;
        end if;
        if LoadBeforeVerify then goto LoadRecord; else goto VerifyToken; end if;
    end either;

\* 2. Load record (remembering its ETag).
LoadRecord:
    rec := records[msg.id];
    if ~rec.exists then Retry();          \* deleted in the meantime - start over
    elsif LoadBeforeVerify then goto VerifyToken;
    else goto UpdateRecord;
    end if;

\* 3. Verify the incoming token exists. No token = duplicate.
VerifyToken:
    if msg.token \notin tokens then goto DiscardDuplicate;
    elsif LoadBeforeVerify then goto UpdateRecord;
    else goto LoadRecord;
    end if;

\* 4. Update record & store outgoing messages, atomically with the business change,
\*    conditionally on the ETag loaded in step 2 (optimistic concurrency).
UpdateRecord:
    if rec.processed then goto CreateTokens;
    else
        either Crash();
        or
            if records[msg.id].exists /\ records[msg.id].ver = rec.ver then
                \* With an extension the id of the first token is generated here and
                \* stored in the same write - no extra round trip on the happy path.
                if Registers /\ Out(msg.id) # {} then
                    myToken := nextToken;
                    holding[self] := IF UseSweeper THEN nextToken ELSE 0;
                    nextToken := nextToken + 1;
                    if Register then
                        registrations[msg.id] := registrations[msg.id] \cup {myToken};
                    end if;
                end if;
                rec := [rec EXCEPT !.processed = TRUE, !.ver = NewVer(rec),
                                   !.initToken = IF UseSweeper THEN myToken ELSE 0];
                records[msg.id] := rec;
                commits[msg.id] := commits[msg.id] + 1;
                nextVer := nextVer + 1;
                goto CreateTokens;
            else Retry();                 \* ETag mismatch - someone else got there first
            end if;
        end either;
    end if;

\* 5. Create tokens for the outgoing messages (unless already checkpointed).
CreateTokens:
    if Out(msg.id) = {} then goto DeleteToken;
    elsif UseCheckpoint /\ rec.checkpointed then goto Send;
    elsif Registers /\ myToken = 0 then goto RegisterToken;
    else
        either Crash();
        or
            if ~Registers then
                myToken := IF FreshTokenIds THEN nextToken ELSE msg.id + 1;
                nextToken := nextToken + 1;
            end if;
            tokens := tokens \cup {myToken};
            if UseCheckpoint then goto Checkpoint; else goto Send; end if;
        end either;
    end if;

\* 5a. Re-generation: register a fresh id, regardless of the record's ETag, but only
\*     while the record is processed and not yet checkpointed; read the record in the
\*     same operation (one SQL statement / one transactional batch). Registering does
\*     not change the record's ETag, so it can't make anybody's checkpoint fail.
RegisterToken:
    either Crash();
    or
        if records[msg.id].exists /\ records[msg.id].processed
           /\ ~records[msg.id].checkpointed then
            if Register then
                registrations[msg.id] := registrations[msg.id] \cup {nextToken};
            else
                tokenRows := tokenRows \cup {[tok |-> nextToken, msg |-> msg.id]};
            end if;
            rec := records[msg.id];
            myToken := nextToken;
            holding[self] := IF UseSweeper THEN nextToken ELSE 0;
            nextToken := nextToken + 1;
            goto CreateTokens;
        elsif records[msg.id].exists /\ records[msg.id].checkpointed then
            rec := records[msg.id];               \* someone else checkpointed meanwhile
            goto Send;
        else Retry();                             \* record gone or re-created: not ours any more
        end if;
    end either;

\* 6. Checkpoint: persist the ids of the created tokens in the record (ETag-conditional).
\*    From now on the outgoing messages always carry exactly these tokens.
Checkpoint:
    either Crash();
    or
        if records[msg.id].exists /\ records[msg.id].ver = rec.ver then
            rec := [rec EXCEPT !.checkpointed = TRUE, !.token = myToken, !.ver = NewVer(rec)];
            records[msg.id] := rec;
            nextVer := nextVer + 1;
            goto Send;
        else goto ReleaseTokens;
        end if;
    end either;

\* Checkpoint lost the race: the tokens we created can never be referenced
\* by any message, so it is safe to remove them right away.
ReleaseTokens:
    either Crash();
    or
        tokens := tokens \ {myToken};
        Retry();
    end either;

\* 7. Send the outgoing messages with the checkpointed tokens.
Send:
    either Crash();
    or
        with out = [id |-> msg.id + 1,
                    token |-> IF UseCheckpoint THEN rec.token ELSE myToken] do
            queue := queue (+) One(out);
            sent := sent \cup {out};
        end with;
        goto DeleteToken;
    end either;

\* 8. Delete the incoming token - from now on every copy of the message is a duplicate.
DeleteToken:
    either Crash();
    or
        tokens := tokens \ {msg.token};
        if Register then goto CleanupRead; else goto DeleteRecord; end if;
    end either;

\* 8a. (Register) Read the record and its registrations, and delete the
\*     registered-but-not-checkpointed tokens. No new registration can appear after
\*     this read: registering requires a processed, not checkpointed record, while
\*     cleanup only runs on a checkpointed (or fresh, unprocessed) one.
CleanupRead:
    rec := records[msg.id];
    regs := registrations[msg.id];
    if ~rec.exists then goto Ack; else goto DeleteOrphans; end if;

DeleteOrphans:
    either Crash();
    or
        tokens := tokens \ Orphans(regs, rec);
        goto DeleteRecord;
    end either;

\* 9. Delete the transaction record - nothing is left behind.
\*    (Register) together with its registrations.
\*    (Sweeper) in ONE transaction: delete the row of the checkpointed set (it holds
\*    live tokens), turn not-checkpointed initial ids into a row for the sweeper, and
\*    delete the record. "SweeperRowAfterRecord" deletes the checkpointed row only
\*    afterwards, in a separate step.
DeleteRecord:
    either Crash();
    or
        if UseSweeper then
            rec := records[msg.id];
            if OrphanCleanup = "SweeperRowAfterRecord" then
                tokenRows := tokenRows \cup InitialRows(rec, msg.id);
                records[msg.id] := NoRecord;
                goto DeleteCheckpointedRow;
            else
                tokenRows := (tokenRows \ CheckpointedRows(rec, msg.id))
                             \cup InitialRows(rec, msg.id);
                records[msg.id] := NoRecord;
                goto Ack;
            end if;
        else
            records[msg.id] := NoRecord;
            registrations[msg.id] := {};
            goto Ack;
        end if;
    end either;

\* ("SweeperRowAfterRecord" only) the broken order: checkpointed row after the record.
DeleteCheckpointedRow:
    either Crash();
    or
        tokenRows := tokenRows \ CheckpointedRows(rec, msg.id);
        goto Ack;
    end either;

\* Duplicate: clean up the record we may have (re-)created and drop the message.
DiscardDuplicate:
    if Register then goto CleanupRead;
    elsif UseSweeper then goto DeleteRecord;
    else
        either Crash();
        or
            records[msg.id] := NoRecord;
            registrations[msg.id] := {};
            goto Ack;
        end either;
    end if;

\* Acknowledge the delivery. It was taken out of the queue in MainLoop; a crash
\* before this point makes it visible again.
Ack:
    either Crash();
    or
        goto MainLoop;
    end either;
end process;

\* At-least-once delivery: the broker may deliver an extra copy of any message
\* ever sent, at any time - also long after the original was processed.
process Broker = BrokerId
begin
Redeliver:
    while duplicates < MaxDuplicates do
        with m \in sent do
            queue := queue (+) One(m);
        end with;
        duplicates := duplicates + 1;
    end while;
end process;

\* (Sweeper) Periodic sweeper. For every row whose record no longer exists:
\*   - delete its token (always safe: a non-checkpointed set is never referenced by a
\*     message, and the row of the checkpointed set is removed before the record);
\*   - delete the row itself only after the grace period D. "Older than D" is modelled
\*     as: the handler that registered the id is no longer in the middle of an attempt
\*     with it, so it can't create the token any more. "SweeperNoGrace" deletes rows
\*     right away.
\* Deletes are idempotent, so a crashing sweeper is just a sweeper that runs again.
\* Without a sweeper mode the process stays blocked (no extra interleavings).
fair process Sweeper = SweeperId
begin
Sweep:
    while TRUE do
        await UseSweeper;
        with r \in {x \in tokenRows :
                      /\ ~records[x.msg].exists
                      /\ \/ x.tok \in tokens
                         \/ OrphanCleanup = "SweeperNoGrace"
                         \/ \A h \in Handlers : ~(pc[h] # "MainLoop" /\ holding[h] = x.tok)} do
            if r.tok \in tokens then
                tokens := tokens \ {r.tok};
            else
                tokenRows := tokenRows \ {r};
            end if;
        end with;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION
VARIABLES queue, sent, tokens, records, registrations, tokenRows, holding, 
          commits, nextVer, nextToken, failures, duplicates, pc

(* define statement *)
NewVer(r)  == IF UniqueETags THEN nextVer ELSE r.ver + 1
SentTokens == {m.token : m \in sent}
Quiescent  == BagToSet(queue) = {} /\ \A h \in Handlers : pc[h] = "MainLoop"



CheckpointedRows(r, k) ==
    IF r.checkpointed THEN {x \in tokenRows : x.msg = k /\ x.tok = r.token} ELSE {}

InitialRows(r, k) ==
    IF /\ r.initToken # 0
       /\ ~(r.checkpointed /\ r.token = r.initToken)
       /\ OrphanCleanup # "SweeperDropInitialIds"
    THEN {[tok |-> r.initToken, msg |-> k]} ELSE {}

TypeOK ==
    /\ BagToSet(queue) \subseteq sent
    /\ \A m \in sent : m.id \in MsgIds /\ m.token \in Nat
    /\ tokens \subseteq Nat
    /\ tokenRows \subseteq [tok : Nat, msg : MsgIds]
    /\ \A k \in MsgIds : commits[k] \in Nat


NoDuplicateProcessing == \A k \in MsgIds : commits[k] <= 1


NoGhostMessages == \A m \in sent : m.id > 1 => commits[m.id - 1] >= 1


ConsistentTokens == \A m1, m2 \in sent : m1.id = m2.id => m1.token = m2.token



NoLostMessages == \A m \in sent : commits[m.id] = 0 => m.token \in tokens


NoLeftoverRecords == Quiescent => \A k \in MsgIds : ~records[k].exists



NoLeftoverTokens == Quiescent => /\ tokens \cap SentTokens = {}
                                 /\ Cardinality(tokens) <= failures


NoOrphanTokens == Quiescent => tokens = {}



NoOrphanTokensAfterSweep == (Quiescent /\ tokenRows = {}) => tokens = {}


AllProcessed == <>(\A k \in MsgIds : commits[k] >= 1)



OrphansSwept == Quiescent ~> (tokens = {} /\ tokenRows = {})

VARIABLES msg, rec, myToken, regs

vars == << queue, sent, tokens, records, registrations, tokenRows, holding, 
           commits, nextVer, nextToken, failures, duplicates, pc, msg, rec, 
           myToken, regs >>

ProcSet == (Handlers) \cup {BrokerId} \cup {SweeperId}

Init == (* Global variables *)
        /\ queue = One([id |-> 1, token |-> InitialToken])
        /\ sent = {[id |-> 1, token |-> InitialToken]}
        /\ tokens = {InitialToken}
        /\ records = [k \in MsgIds |-> NoRecord]
        /\ registrations = [k \in MsgIds |-> {}]
        /\ tokenRows = {}
        /\ holding = [h \in Handlers |-> 0]
        /\ commits = [k \in MsgIds |-> 0]
        /\ nextVer = 1
        /\ nextToken = ChainLength + 1
        /\ failures = 0
        /\ duplicates = 0
        (* Process Handler *)
        /\ msg = [self \in Handlers |-> [id |-> 1, token |-> InitialToken]]
        /\ rec = [self \in Handlers |-> NoRecord]
        /\ myToken = [self \in Handlers |-> 0]
        /\ regs = [self \in Handlers |-> {}]
        /\ pc = [self \in ProcSet |-> CASE self \in Handlers -> "MainLoop"
                                        [] self = BrokerId -> "Redeliver"
                                        [] self = SweeperId -> "Sweep"]

MainLoop(self) == /\ pc[self] = "MainLoop"
                  /\ BagToSet(queue) # {}
                  /\ \E m \in BagToSet(queue):
                       /\ msg' = [msg EXCEPT ![self] = m]
                       /\ queue' = queue (-) One(m)
                  /\ rec' = [rec EXCEPT ![self] = NoRecord]
                  /\ myToken' = [myToken EXCEPT ![self] = 0]
                  /\ regs' = [regs EXCEPT ![self] = {}]
                  /\ holding' = [holding EXCEPT ![self] = 0]
                  /\ pc' = [pc EXCEPT ![self] = "CreateRecord"]
                  /\ UNCHANGED << sent, tokens, records, registrations, 
                                  tokenRows, commits, nextVer, nextToken, 
                                  failures, duplicates >>

CreateRecord(self) == /\ pc[self] = "CreateRecord"
                      /\ \/ /\ failures < MaxFailures
                            /\ failures' = failures + 1
                            /\ queue' = queue (+) One(msg[self])
                            /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                            /\ UNCHANGED <<records, nextVer>>
                         \/ /\ IF ~records[msg[self].id].exists
                                  THEN /\ records' = [records EXCEPT ![msg[self].id] = [NoRecord EXCEPT !.exists = TRUE,
                                                                                                        !.ver = IF UniqueETags THEN nextVer ELSE 0]]
                                       /\ nextVer' = nextVer + 1
                                  ELSE /\ TRUE
                                       /\ UNCHANGED << records, nextVer >>
                            /\ IF LoadBeforeVerify
                                  THEN /\ pc' = [pc EXCEPT ![self] = "LoadRecord"]
                                  ELSE /\ pc' = [pc EXCEPT ![self] = "VerifyToken"]
                            /\ UNCHANGED <<queue, failures>>
                      /\ UNCHANGED << sent, tokens, registrations, tokenRows, 
                                      holding, commits, nextToken, duplicates, 
                                      msg, rec, myToken, regs >>

LoadRecord(self) == /\ pc[self] = "LoadRecord"
                    /\ rec' = [rec EXCEPT ![self] = records[msg[self].id]]
                    /\ IF ~rec'[self].exists
                          THEN /\ queue' = queue (+) One(msg[self])
                               /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                          ELSE /\ IF LoadBeforeVerify
                                     THEN /\ pc' = [pc EXCEPT ![self] = "VerifyToken"]
                                     ELSE /\ pc' = [pc EXCEPT ![self] = "UpdateRecord"]
                               /\ queue' = queue
                    /\ UNCHANGED << sent, tokens, records, registrations, 
                                    tokenRows, holding, commits, nextVer, 
                                    nextToken, failures, duplicates, msg, 
                                    myToken, regs >>

VerifyToken(self) == /\ pc[self] = "VerifyToken"
                     /\ IF msg[self].token \notin tokens
                           THEN /\ pc' = [pc EXCEPT ![self] = "DiscardDuplicate"]
                           ELSE /\ IF LoadBeforeVerify
                                      THEN /\ pc' = [pc EXCEPT ![self] = "UpdateRecord"]
                                      ELSE /\ pc' = [pc EXCEPT ![self] = "LoadRecord"]
                     /\ UNCHANGED << queue, sent, tokens, records, 
                                     registrations, tokenRows, holding, 
                                     commits, nextVer, nextToken, failures, 
                                     duplicates, msg, rec, myToken, regs >>

UpdateRecord(self) == /\ pc[self] = "UpdateRecord"
                      /\ IF rec[self].processed
                            THEN /\ pc' = [pc EXCEPT ![self] = "CreateTokens"]
                                 /\ UNCHANGED << queue, records, registrations, 
                                                 holding, commits, nextVer, 
                                                 nextToken, failures, rec, 
                                                 myToken >>
                            ELSE /\ \/ /\ failures < MaxFailures
                                       /\ failures' = failures + 1
                                       /\ queue' = queue (+) One(msg[self])
                                       /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                                       /\ UNCHANGED <<records, registrations, holding, commits, nextVer, nextToken, rec, myToken>>
                                    \/ /\ IF records[msg[self].id].exists /\ records[msg[self].id].ver = rec[self].ver
                                             THEN /\ IF Registers /\ Out(msg[self].id) # {}
                                                        THEN /\ myToken' = [myToken EXCEPT ![self] = nextToken]
                                                             /\ holding' = [holding EXCEPT ![self] = IF UseSweeper THEN nextToken ELSE 0]
                                                             /\ nextToken' = nextToken + 1
                                                             /\ IF Register
                                                                   THEN /\ registrations' = [registrations EXCEPT ![msg[self].id] = registrations[msg[self].id] \cup {myToken'[self]}]
                                                                   ELSE /\ TRUE
                                                                        /\ UNCHANGED registrations
                                                        ELSE /\ TRUE
                                                             /\ UNCHANGED << registrations, 
                                                                             holding, 
                                                                             nextToken, 
                                                                             myToken >>
                                                  /\ rec' = [rec EXCEPT ![self] = [rec[self] EXCEPT !.processed = TRUE, !.ver = NewVer(rec[self]),
                                                                                                    !.initToken = IF UseSweeper THEN myToken'[self] ELSE 0]]
                                                  /\ records' = [records EXCEPT ![msg[self].id] = rec'[self]]
                                                  /\ commits' = [commits EXCEPT ![msg[self].id] = commits[msg[self].id] + 1]
                                                  /\ nextVer' = nextVer + 1
                                                  /\ pc' = [pc EXCEPT ![self] = "CreateTokens"]
                                                  /\ queue' = queue
                                             ELSE /\ queue' = queue (+) One(msg[self])
                                                  /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                                                  /\ UNCHANGED << records, 
                                                                  registrations, 
                                                                  holding, 
                                                                  commits, 
                                                                  nextVer, 
                                                                  nextToken, 
                                                                  rec, myToken >>
                                       /\ UNCHANGED failures
                      /\ UNCHANGED << sent, tokens, tokenRows, duplicates, msg, 
                                      regs >>

CreateTokens(self) == /\ pc[self] = "CreateTokens"
                      /\ IF Out(msg[self].id) = {}
                            THEN /\ pc' = [pc EXCEPT ![self] = "DeleteToken"]
                                 /\ UNCHANGED << queue, tokens, nextToken, 
                                                 failures, myToken >>
                            ELSE /\ IF UseCheckpoint /\ rec[self].checkpointed
                                       THEN /\ pc' = [pc EXCEPT ![self] = "Send"]
                                            /\ UNCHANGED << queue, tokens, 
                                                            nextToken, 
                                                            failures, myToken >>
                                       ELSE /\ IF Registers /\ myToken[self] = 0
                                                  THEN /\ pc' = [pc EXCEPT ![self] = "RegisterToken"]
                                                       /\ UNCHANGED << queue, 
                                                                       tokens, 
                                                                       nextToken, 
                                                                       failures, 
                                                                       myToken >>
                                                  ELSE /\ \/ /\ failures < MaxFailures
                                                             /\ failures' = failures + 1
                                                             /\ queue' = queue (+) One(msg[self])
                                                             /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                                                             /\ UNCHANGED <<tokens, nextToken, myToken>>
                                                          \/ /\ IF ~Registers
                                                                   THEN /\ myToken' = [myToken EXCEPT ![self] = IF FreshTokenIds THEN nextToken ELSE msg[self].id + 1]
                                                                        /\ nextToken' = nextToken + 1
                                                                   ELSE /\ TRUE
                                                                        /\ UNCHANGED << nextToken, 
                                                                                        myToken >>
                                                             /\ tokens' = (tokens \cup {myToken'[self]})
                                                             /\ IF UseCheckpoint
                                                                   THEN /\ pc' = [pc EXCEPT ![self] = "Checkpoint"]
                                                                   ELSE /\ pc' = [pc EXCEPT ![self] = "Send"]
                                                             /\ UNCHANGED <<queue, failures>>
                      /\ UNCHANGED << sent, records, registrations, tokenRows, 
                                      holding, commits, nextVer, duplicates, 
                                      msg, rec, regs >>

RegisterToken(self) == /\ pc[self] = "RegisterToken"
                       /\ \/ /\ failures < MaxFailures
                             /\ failures' = failures + 1
                             /\ queue' = queue (+) One(msg[self])
                             /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                             /\ UNCHANGED <<registrations, tokenRows, holding, nextToken, rec, myToken>>
                          \/ /\ IF records[msg[self].id].exists /\ records[msg[self].id].processed
                                   /\ ~records[msg[self].id].checkpointed
                                   THEN /\ IF Register
                                              THEN /\ registrations' = [registrations EXCEPT ![msg[self].id] = registrations[msg[self].id] \cup {nextToken}]
                                                   /\ UNCHANGED tokenRows
                                              ELSE /\ tokenRows' = (tokenRows \cup {[tok |-> nextToken, msg |-> msg[self].id]})
                                                   /\ UNCHANGED registrations
                                        /\ rec' = [rec EXCEPT ![self] = records[msg[self].id]]
                                        /\ myToken' = [myToken EXCEPT ![self] = nextToken]
                                        /\ holding' = [holding EXCEPT ![self] = IF UseSweeper THEN nextToken ELSE 0]
                                        /\ nextToken' = nextToken + 1
                                        /\ pc' = [pc EXCEPT ![self] = "CreateTokens"]
                                        /\ queue' = queue
                                   ELSE /\ IF records[msg[self].id].exists /\ records[msg[self].id].checkpointed
                                              THEN /\ rec' = [rec EXCEPT ![self] = records[msg[self].id]]
                                                   /\ pc' = [pc EXCEPT ![self] = "Send"]
                                                   /\ queue' = queue
                                              ELSE /\ queue' = queue (+) One(msg[self])
                                                   /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                                                   /\ rec' = rec
                                        /\ UNCHANGED << registrations, 
                                                        tokenRows, holding, 
                                                        nextToken, myToken >>
                             /\ UNCHANGED failures
                       /\ UNCHANGED << sent, tokens, records, commits, nextVer, 
                                       duplicates, msg, regs >>

Checkpoint(self) == /\ pc[self] = "Checkpoint"
                    /\ \/ /\ failures < MaxFailures
                          /\ failures' = failures + 1
                          /\ queue' = queue (+) One(msg[self])
                          /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                          /\ UNCHANGED <<records, nextVer, rec>>
                       \/ /\ IF records[msg[self].id].exists /\ records[msg[self].id].ver = rec[self].ver
                                THEN /\ rec' = [rec EXCEPT ![self] = [rec[self] EXCEPT !.checkpointed = TRUE, !.token = myToken[self], !.ver = NewVer(rec[self])]]
                                     /\ records' = [records EXCEPT ![msg[self].id] = rec'[self]]
                                     /\ nextVer' = nextVer + 1
                                     /\ pc' = [pc EXCEPT ![self] = "Send"]
                                ELSE /\ pc' = [pc EXCEPT ![self] = "ReleaseTokens"]
                                     /\ UNCHANGED << records, nextVer, rec >>
                          /\ UNCHANGED <<queue, failures>>
                    /\ UNCHANGED << sent, tokens, registrations, tokenRows, 
                                    holding, commits, nextToken, duplicates, 
                                    msg, myToken, regs >>

ReleaseTokens(self) == /\ pc[self] = "ReleaseTokens"
                       /\ \/ /\ failures < MaxFailures
                             /\ failures' = failures + 1
                             /\ queue' = queue (+) One(msg[self])
                             /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                             /\ UNCHANGED tokens
                          \/ /\ tokens' = tokens \ {myToken[self]}
                             /\ queue' = queue (+) One(msg[self])
                             /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                             /\ UNCHANGED failures
                       /\ UNCHANGED << sent, records, registrations, tokenRows, 
                                       holding, commits, nextVer, nextToken, 
                                       duplicates, msg, rec, myToken, regs >>

Send(self) == /\ pc[self] = "Send"
              /\ \/ /\ failures < MaxFailures
                    /\ failures' = failures + 1
                    /\ queue' = queue (+) One(msg[self])
                    /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                    /\ sent' = sent
                 \/ /\ LET out == [id |-> msg[self].id + 1,
                                   token |-> IF UseCheckpoint THEN rec[self].token ELSE myToken[self]] IN
                         /\ queue' = queue (+) One(out)
                         /\ sent' = (sent \cup {out})
                    /\ pc' = [pc EXCEPT ![self] = "DeleteToken"]
                    /\ UNCHANGED failures
              /\ UNCHANGED << tokens, records, registrations, tokenRows, 
                              holding, commits, nextVer, nextToken, duplicates, 
                              msg, rec, myToken, regs >>

DeleteToken(self) == /\ pc[self] = "DeleteToken"
                     /\ \/ /\ failures < MaxFailures
                           /\ failures' = failures + 1
                           /\ queue' = queue (+) One(msg[self])
                           /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                           /\ UNCHANGED tokens
                        \/ /\ tokens' = tokens \ {msg[self].token}
                           /\ IF Register
                                 THEN /\ pc' = [pc EXCEPT ![self] = "CleanupRead"]
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "DeleteRecord"]
                           /\ UNCHANGED <<queue, failures>>
                     /\ UNCHANGED << sent, records, registrations, tokenRows, 
                                     holding, commits, nextVer, nextToken, 
                                     duplicates, msg, rec, myToken, regs >>

CleanupRead(self) == /\ pc[self] = "CleanupRead"
                     /\ rec' = [rec EXCEPT ![self] = records[msg[self].id]]
                     /\ regs' = [regs EXCEPT ![self] = registrations[msg[self].id]]
                     /\ IF ~rec'[self].exists
                           THEN /\ pc' = [pc EXCEPT ![self] = "Ack"]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "DeleteOrphans"]
                     /\ UNCHANGED << queue, sent, tokens, records, 
                                     registrations, tokenRows, holding, 
                                     commits, nextVer, nextToken, failures, 
                                     duplicates, msg, myToken >>

DeleteOrphans(self) == /\ pc[self] = "DeleteOrphans"
                       /\ \/ /\ failures < MaxFailures
                             /\ failures' = failures + 1
                             /\ queue' = queue (+) One(msg[self])
                             /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                             /\ UNCHANGED tokens
                          \/ /\ tokens' = tokens \ Orphans(regs[self], rec[self])
                             /\ pc' = [pc EXCEPT ![self] = "DeleteRecord"]
                             /\ UNCHANGED <<queue, failures>>
                       /\ UNCHANGED << sent, records, registrations, tokenRows, 
                                       holding, commits, nextVer, nextToken, 
                                       duplicates, msg, rec, myToken, regs >>

DeleteRecord(self) == /\ pc[self] = "DeleteRecord"
                      /\ \/ /\ failures < MaxFailures
                            /\ failures' = failures + 1
                            /\ queue' = queue (+) One(msg[self])
                            /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                            /\ UNCHANGED <<records, registrations, tokenRows, rec>>
                         \/ /\ IF UseSweeper
                                  THEN /\ rec' = [rec EXCEPT ![self] = records[msg[self].id]]
                                       /\ IF OrphanCleanup = "SweeperRowAfterRecord"
                                             THEN /\ tokenRows' = (tokenRows \cup InitialRows(rec'[self], msg[self].id))
                                                  /\ records' = [records EXCEPT ![msg[self].id] = NoRecord]
                                                  /\ pc' = [pc EXCEPT ![self] = "DeleteCheckpointedRow"]
                                             ELSE /\ tokenRows' = (tokenRows \ CheckpointedRows(rec'[self], msg[self].id))
                                                                  \cup InitialRows(rec'[self], msg[self].id)
                                                  /\ records' = [records EXCEPT ![msg[self].id] = NoRecord]
                                                  /\ pc' = [pc EXCEPT ![self] = "Ack"]
                                       /\ UNCHANGED registrations
                                  ELSE /\ records' = [records EXCEPT ![msg[self].id] = NoRecord]
                                       /\ registrations' = [registrations EXCEPT ![msg[self].id] = {}]
                                       /\ pc' = [pc EXCEPT ![self] = "Ack"]
                                       /\ UNCHANGED << tokenRows, rec >>
                            /\ UNCHANGED <<queue, failures>>
                      /\ UNCHANGED << sent, tokens, holding, commits, nextVer, 
                                      nextToken, duplicates, msg, myToken, 
                                      regs >>

DeleteCheckpointedRow(self) == /\ pc[self] = "DeleteCheckpointedRow"
                               /\ \/ /\ failures < MaxFailures
                                     /\ failures' = failures + 1
                                     /\ queue' = queue (+) One(msg[self])
                                     /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                                     /\ UNCHANGED tokenRows
                                  \/ /\ tokenRows' = tokenRows \ CheckpointedRows(rec[self], msg[self].id)
                                     /\ pc' = [pc EXCEPT ![self] = "Ack"]
                                     /\ UNCHANGED <<queue, failures>>
                               /\ UNCHANGED << sent, tokens, records, 
                                               registrations, holding, commits, 
                                               nextVer, nextToken, duplicates, 
                                               msg, rec, myToken, regs >>

DiscardDuplicate(self) == /\ pc[self] = "DiscardDuplicate"
                          /\ IF Register
                                THEN /\ pc' = [pc EXCEPT ![self] = "CleanupRead"]
                                     /\ UNCHANGED << queue, records, 
                                                     registrations, failures >>
                                ELSE /\ IF UseSweeper
                                           THEN /\ pc' = [pc EXCEPT ![self] = "DeleteRecord"]
                                                /\ UNCHANGED << queue, records, 
                                                                registrations, 
                                                                failures >>
                                           ELSE /\ \/ /\ failures < MaxFailures
                                                      /\ failures' = failures + 1
                                                      /\ queue' = queue (+) One(msg[self])
                                                      /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                                                      /\ UNCHANGED <<records, registrations>>
                                                   \/ /\ records' = [records EXCEPT ![msg[self].id] = NoRecord]
                                                      /\ registrations' = [registrations EXCEPT ![msg[self].id] = {}]
                                                      /\ pc' = [pc EXCEPT ![self] = "Ack"]
                                                      /\ UNCHANGED <<queue, failures>>
                          /\ UNCHANGED << sent, tokens, tokenRows, holding, 
                                          commits, nextVer, nextToken, 
                                          duplicates, msg, rec, myToken, regs >>

Ack(self) == /\ pc[self] = "Ack"
             /\ \/ /\ failures < MaxFailures
                   /\ failures' = failures + 1
                   /\ queue' = queue (+) One(msg[self])
                   /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                \/ /\ pc' = [pc EXCEPT ![self] = "MainLoop"]
                   /\ UNCHANGED <<queue, failures>>
             /\ UNCHANGED << sent, tokens, records, registrations, tokenRows, 
                             holding, commits, nextVer, nextToken, duplicates, 
                             msg, rec, myToken, regs >>

Handler(self) == MainLoop(self) \/ CreateRecord(self) \/ LoadRecord(self)
                    \/ VerifyToken(self) \/ UpdateRecord(self)
                    \/ CreateTokens(self) \/ RegisterToken(self)
                    \/ Checkpoint(self) \/ ReleaseTokens(self)
                    \/ Send(self) \/ DeleteToken(self) \/ CleanupRead(self)
                    \/ DeleteOrphans(self) \/ DeleteRecord(self)
                    \/ DeleteCheckpointedRow(self)
                    \/ DiscardDuplicate(self) \/ Ack(self)

Redeliver == /\ pc[BrokerId] = "Redeliver"
             /\ IF duplicates < MaxDuplicates
                   THEN /\ \E m \in sent:
                             queue' = queue (+) One(m)
                        /\ duplicates' = duplicates + 1
                        /\ pc' = [pc EXCEPT ![BrokerId] = "Redeliver"]
                   ELSE /\ pc' = [pc EXCEPT ![BrokerId] = "Done"]
                        /\ UNCHANGED << queue, duplicates >>
             /\ UNCHANGED << sent, tokens, records, registrations, tokenRows, 
                             holding, commits, nextVer, nextToken, failures, 
                             msg, rec, myToken, regs >>

Broker == Redeliver

Sweep == /\ pc[SweeperId] = "Sweep"
         /\ UseSweeper
         /\ \E r \in {x \in tokenRows :
                        /\ ~records[x.msg].exists
                        /\ \/ x.tok \in tokens
                           \/ OrphanCleanup = "SweeperNoGrace"
                           \/ \A h \in Handlers : ~(pc[h] # "MainLoop" /\ holding[h] = x.tok)}:
              IF r.tok \in tokens
                 THEN /\ tokens' = tokens \ {r.tok}
                      /\ UNCHANGED tokenRows
                 ELSE /\ tokenRows' = tokenRows \ {r}
                      /\ UNCHANGED tokens
         /\ pc' = [pc EXCEPT ![SweeperId] = "Sweep"]
         /\ UNCHANGED << queue, sent, records, registrations, holding, commits, 
                         nextVer, nextToken, failures, duplicates, msg, rec, 
                         myToken, regs >>

Sweeper == Sweep

Next == Broker \/ Sweeper
           \/ (\E self \in Handlers: Handler(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Handlers : WF_vars(Handler(self))
        /\ WF_vars(Sweeper)

\* END TRANSLATION
=============================================================================
