# Queue Replication Specification

## Purpose

Defines the Raft-lite consensus state machine that makes the queue's log a single
replicated source of truth across a cluster of `sdypp_colas_serv` nodes: terms, voting,
the append-only log, election safety, heartbeats, replication, commit-index advancement,
and term fencing. This is pure state-machine logic — driven by an injectable clock — with
no HTTP transport, no sockets, and no real timers. HTTP wiring for `/raft/*` is covered by
`queue-service-api`; how committed entries mutate `colas.py` state is covered by
`queue-log-application`.

## Requirements

### Requirement: Node Roles and Persistent Term State

Each node MUST maintain, in memory, `termino_actual` (current term, monotonically
non-decreasing), `voto_para` (the candidate voted for in the current term, or none), and
an append-only log of entries, each carrying `{indice, termino, operacion, payload}`. Each
node MUST be in exactly one role at any instant: `master`, `slave`, or `candidato`.

#### Scenario: A freshly started node with an empty seed list of size N>1 starts as slave

- GIVEN a node starting with `termino_actual = 0`, no prior log, and a seed list with 2 or
  more peers
- WHEN the node initializes
- THEN its role MUST be `slave`
- AND it MUST wait for either a heartbeat/appendEntries from a master or its own election
  timeout to elapse

#### Scenario: Term only increases, never decreases

- GIVEN a node with `termino_actual = 7`
- WHEN the node receives any Raft message (heartbeat, appendEntries, requestVote) carrying
  a term less than or equal to 7
- THEN the node's `termino_actual` MUST NOT decrease
- AND the node MUST NOT grant a vote for a term less than or equal to its own current term

### Requirement: Election with Majority and Log-Freshness Vote Rule

A node MUST start an election when its election timeout elapses without receiving a valid
heartbeat or `appendEntries` from a recognized master. Starting an election MUST increment
`termino_actual` by 1, set `voto_para` to itself, transition role to `candidato`, and
request votes from all peers with the candidate's term, id, last log index, and last log
term. A voter MUST grant a vote in a given term at most once, and only if the candidate's
log is **at least as up to date** as the voter's own — compared first by the term of the
last log entry, then, on a tie, by log index. A candidate that receives grants from a
majority of the cluster (`ceil((N+1)/2)` out of N total nodes, counting itself) MUST
transition to `master` and immediately begin sending heartbeats under its new term.

#### Scenario: Candidate with a stale log cannot win

- GIVEN a 3-node cluster where node A's log ends at `{indice: 5, termino: 3}` and node B's
  log ends at `{indice: 6, termino: 4}`
- WHEN node A starts an election for term 5 and requests a vote from node B
- THEN node B MUST refuse the vote, because A's last log term (3) is lower than B's last
  log term (4)

#### Scenario: Candidate with an equally fresh but shorter log loses the tiebreak

- GIVEN two candidates whose last log entries share `termino: 4`, one with `indice: 6` and
  the other with `indice: 5`
- WHEN a voter with last log entry `{indice: 6, termino: 4}` evaluates a requestVote from
  the candidate with `{indice: 5, termino: 4}`
- THEN the voter MUST refuse the vote, because the candidate's log index is behind the
  voter's own at the same term

#### Scenario: Majority election succeeds at 3 nodes

- GIVEN a 3-node cluster with no current master and all logs empty
- WHEN one node's election timeout elapses first and it requests votes from the other two
- THEN it MUST receive at least 1 additional vote (2 total, a majority of 3) to become
  master
- AND the other two nodes MUST observe the new master's term and transition to `slave`

#### Scenario: Majority election succeeds at 5 nodes

- GIVEN a 5-node cluster with no current master and all logs empty
- WHEN one node's election timeout elapses first and it requests votes from the other four
- THEN it MUST receive at least 2 additional votes (3 total, a majority of 5) to become
  master

#### Scenario: A node votes at most once per term

- GIVEN a node at `termino_actual = 5` that already voted for candidate A in term 5
- WHEN a different candidate B requests a vote for term 5 with an equally or more
  up-to-date log
- THEN the node MUST refuse B's vote

### Requirement: Term Fencing (No Split-Brain)

Every message exchanged between cluster nodes (heartbeat, `appendEntries`, `requestVote`)
MUST carry the sender's term. A node that observes a term strictly greater than its own
`termino_actual` MUST immediately update `termino_actual` to that value, clear `voto_para`
for the new term, and step down to `slave` regardless of its current role — including if it
was `master`. A stepped-down former master MUST stop accepting new log entries and MUST
stop counting any in-flight commit toward a majority for entries not yet committed under
its old term.

#### Scenario: A master sees a higher term and steps down

- GIVEN a node acting as `master` at `termino_actual = 4`
- WHEN it receives an `appendEntries` or `requestVote` carrying `termino = 5`
- THEN it MUST step down to `slave` and adopt `termino_actual = 5`

#### Scenario: No two masters coexist under a healed partition

- GIVEN a cluster of 3 nodes where a network partition previously isolated one node (the
  old master) from the other two, and the majority side already elected a new master at a
  higher term
- WHEN the partition heals and the old master's messages reach the new majority, or the new
  majority's heartbeat reaches the old master
- THEN the old master MUST observe the higher term and step down to `slave`
- AND at no point during or after partition healing MUST two nodes simultaneously report
  `"rol": "master"` for overlapping wall-clock time once term-fencing has propagated

#### Scenario: No two masters coexist during an active partition

- GIVEN a 3-node cluster split into a majority side (2 nodes) and a minority side (1 node,
  the old master)
- WHEN the partition is active
- THEN the minority-side node MUST NOT be able to commit any new log entry, because it
  cannot reach a majority
- AND the majority side MUST elect a new master within its election timeout

### Requirement: Heartbeats and Election Timeout

The master MUST send periodic `appendEntries` heartbeats (with zero or more new entries) to
every slave at a fixed interval (`RAFT_HEARTBEAT_MS`). Each slave MUST reset its election
timeout upon receiving a valid heartbeat or `appendEntries` from the current or a newer
term's master. Election timeouts MUST be independently randomized (jittered) per node so
that not all slaves start an election simultaneously.

#### Scenario: A slave that stops receiving heartbeats starts an election

- GIVEN a slave that last received a valid heartbeat at time T
- WHEN no valid heartbeat or appendEntries arrives before `T + RAFT_ELECCION_TIMEOUT_MS`
  (plus its jitter)
- THEN the slave MUST transition to `candidato` and start an election

#### Scenario: A slave receiving heartbeats never starts an election

- GIVEN a slave receiving a valid heartbeat from the current master every
  `RAFT_HEARTBEAT_MS`, with `RAFT_HEARTBEAT_MS` well under `RAFT_ELECCION_TIMEOUT_MS`
- WHEN observed over many heartbeat intervals
- THEN the slave's election timeout MUST never elapse and it MUST remain `slave`

### Requirement: Log Replication and Commit-Index Advancement

The master MUST replicate new log entries to slaves via `appendEntries`, in the same order
they were appended locally. A slave receiving `appendEntries` MUST append the new entries to
its own log in order and reply with the highest index it has applied to its log. The master
MUST track, for each entry, how many nodes (including itself) have durably appended it, and
MUST advance its `indiceCommit` to the highest index acknowledged by a majority
(`ceil((N+1)/2)` out of N). An entry MUST NOT be considered committed, and its effect MUST
NOT be applied or acknowledged to any external caller, before a majority holds it.

#### Scenario: An entry commits only after majority acknowledgement

- GIVEN a 3-node cluster (1 master, 2 slaves) and a new log entry appended by the master
- WHEN only the master itself holds the entry (0 of 2 slaves have acknowledged it)
- THEN the master MUST NOT advance `indiceCommit` past that entry
- WHEN at least 1 of the 2 slaves acknowledges the entry
- THEN the master MUST advance `indiceCommit` to include that entry, because master + 1
  slave = 2, a majority of 3

#### Scenario: A slower slave still receives entries it missed, in order

- GIVEN a slave that fell behind by several entries while a partition kept it isolated
- WHEN the partition heals and the master resumes sending `appendEntries`
- THEN the slave MUST receive and apply the missing entries in log order before it can
  acknowledge the master's latest index

### Requirement: Injectable Clock for Deterministic Testing

The state machine (election timeout, heartbeat scheduling, jitter) MUST be driven by an
injectable clock abstraction rather than direct calls to wall-clock time or `time.sleep`.
Tests MUST be able to substitute a fake clock or an explicit "force election now" hook to
deterministically trigger timeouts without relying on real elapsed time.

#### Scenario: A test forces an election without waiting on wall-clock time

- GIVEN a raft state machine instance constructed with a fake, test-controlled clock
- WHEN the test advances the fake clock past the configured election timeout, or invokes
  the explicit force-election hook
- THEN the state machine MUST behave identically to a real election timeout having elapsed,
  with no real-time `sleep` or wait involved in the test
