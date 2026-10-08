# Queue Log Application Specification

## Purpose

Defines how committed Raft log entries apply to `colas.py` state: the operation
vocabulary, atomicity of compound mutations, master-only lazy expiry replicated as
`expirar` log entries, and post-election catch-up recovery. `colas.py` business logic
itself is unchanged by this change; what changes is *when* and *by what trigger* it is
invoked — only once an entry is committed (majority-acknowledged), never eagerly.

## Requirements

### Requirement: Mutations Apply Only From Committed Log Entries

Every mutating operation on the queue's business state (`encolar`, `tomar`, `devolver`,
`responder`, `retirar-respuesta`, `expirar`) MUST be represented as a log entry and MUST
apply to `colas.py` state only after that entry is committed (acknowledged by a majority
of the cluster, per `queue-replication`). No mutation MUST be applied speculatively before
commit, and no HTTP response that depends on a mutation's effect MUST be sent before that
entry commits.

#### Scenario: An uncommitted entry has no visible effect

- GIVEN a master that appended a `tomar` entry to its log but has not yet received a
  majority acknowledgement
- WHEN any caller queries queue state that would reflect that `tomar` (e.g. a subsequent
  `tomar` call that should not see the same pedido as available)
- THEN the system MUST behave as if that `tomar` had not happened yet, because it has not
  committed

#### Scenario: A committed entry applies exactly once

- GIVEN a log entry for `encolar` that reaches majority commit
- WHEN the master applies committed entries in order
- THEN that `encolar` MUST be applied to `colas.py` exactly once, even if the
  `appendEntries` round that carried it was retried due to a transient network failure

### Requirement: Compound Mutations Apply Atomically From One Committed Entry

`Sistema.responder()` and `Sistema.recuperar()` each perform two underlying state changes
(pop from the pedidos structure and publish to the responses structure). Each MUST be
represented as a single log entry (`responder`, `expirar`) and MUST apply both of its
constituent mutations atomically when that single entry commits — there MUST be no
intermediate state, observable by any subsequent operation, in which only one of the two
mutations has been applied.

#### Scenario: responder's two mutations are indivisible

- GIVEN a committed `responder` log entry for a given pedido id
- WHEN the master applies that entry
- THEN both the removal from in-flight pedidos and the publication to the destinatario's
  response queue MUST happen as one atomic step
- AND no concurrent read (e.g. `POST /respuestas/tomar` for that destinatario) MUST be able
  to observe a state where the pedido was removed but the response was not yet published,
  or vice versa

#### Scenario: recuperar's expiry-and-refail is indivisible

- GIVEN a committed `expirar` entry that both removes an expired in-flight pedido and either
  re-queues it (idempotent, budget remaining) or fails it with `DEADLINE_EXCEEDED`
- WHEN the master applies that entry
- THEN the removal and the re-queue-or-fail decision MUST apply as one atomic step from that
  single committed entry

### Requirement: Master-Only Lazy Expiry Replicated Through the Log

Only the master MUST run the periodic expiry sweep (the `recuperador()` thread). It MUST
detect expired reservations and expired waiting pedidos using its own monotonic clock, and
MUST replicate each expiry decision as an `expirar` log entry rather than letting slaves
independently recompute expiry against their own clocks. A slave MUST NOT run its own
expiry sweep while it is not master.

#### Scenario: A slave never independently expires a pedido

- GIVEN a 3-node cluster with one master and two slaves
- WHEN a pedido's reservation would appear expired if judged against a slave's own clock,
  but no `expirar` entry has been committed for it yet
- THEN neither slave MUST apply an expiry to its own local view of that pedido; only an
  `expirar` entry replicated from the master MUST cause that

#### Scenario: recuperador() is gated to the master role

- GIVEN a node that just stepped down from master to slave (observed a higher term)
- WHEN the transition completes
- THEN that node's expiry sweep thread MUST stop running, because it is no longer master

#### Scenario: recuperador() starts when a node becomes master

- GIVEN a node that just won an election and became master
- WHEN it completes post-election catch-up (see below)
- THEN its expiry sweep thread MUST start running against its own monotonic clock

### Requirement: Uncommitted Expiry Blocks tomar() Rather Than Being Ignored

When `tomar()` would resolve against a pedido whose expiry has been locally detected (by
the master's clock) but whose corresponding `expirar` entry has not yet committed, the
system MUST block that `tomar()` call until the `expirar` entry commits, rather than
handing out the pedido as if it were still live. This applies only to the pedido the
locally-detected-but-uncommitted expiry concerns; it MUST NOT block `tomar()` calls that
would resolve against a different, unaffected pedido.

The wait MUST be bounded and MUST have an exit on every path. Specifically:

- The wait MUST NOT extend past the caller's own long-poll budget (`espera`). If that budget
  elapses before the `expirar` entry commits, the call MUST return `204` with no body —
  already a legal, expected answer on this route, so no contract change is implied.
- If the node loses mastership while a `tomar()` is blocked on that wait, the wait MUST end
  immediately and the call MUST answer `421 {"error": "no-soy-master", "master": <url-or-null>}`
  rather than continuing to wait for an entry the node can no longer commit.

There is no path on which a blocked `tomar()` waits indefinitely. An `expirar` entry that
never commits — because the node stepped down before committing it — MUST NOT hang the caller.

#### Scenario: The long-poll budget elapses before the expirar entry commits

- GIVEN the head of the waiting queue is a pedido whose `expirar` entry is appended but not
  committed
- WHEN a worker calls `POST /pedidos/tomar` with `espera` seconds and the entry has still not
  committed when that budget elapses
- THEN the call MUST return `204` with no body
- AND it MUST NOT return the expired pedido

#### Scenario: Losing mastership while blocked on an uncommitted expiry

- GIVEN a `tomar()` blocked waiting for an `expirar` entry to commit
- WHEN that node observes a higher term and steps down to slave before the entry commits
- THEN the blocked call MUST end immediately rather than waiting out its budget
- AND it MUST answer `421 {"error": "no-soy-master", "master": <url-or-null>}`

#### Scenario: tomar() waits for an in-flight expirar commit before resolving

- GIVEN the head of the waiting queue is a pedido the master's clock considers expired, and
  the corresponding `expirar` entry has been appended but not yet committed
- WHEN a worker calls `POST /pedidos/tomar`
- THEN the call MUST NOT return that expired pedido
- AND the call MUST wait (within its long-poll budget) until the `expirar` entry commits and
  the queue state reflects its removal, then proceed to serve the next live pedido if one is
  available

#### Scenario: tomar() is not blocked by an unrelated pending expiry
- GIVEN an `expirar` entry in flight for pedido X, and pedido Y at the head of the waiting
  queue is unaffected and live
- WHEN a worker calls `POST /pedidos/tomar` and Y is the correct next pedido to serve
- THEN the call MUST proceed to serve Y without waiting on X's `expirar` commit, provided Y
  itself is unaffected

### Requirement: Post-Election Catch-Up Recovery

A newly elected master MUST run a one-shot recovery pass over its (now up-to-date,
majority-backed) log before serving its first `tomar()` call. This pass MUST resolve any
reservations that expired during the leaderless window (the time between the previous
master's failure and this election's completion) by committing the corresponding `expirar`
entries, so that no already-dead pedido is handed out as live immediately after failover.

#### Scenario: A fresh master does not hand out a pedido that died during the election window

- GIVEN a master failed while a pedido was reserved with `reservado_hasta` in the near
  future, and the leaderless window (election time) exceeded that deadline
- WHEN a new master is elected
- THEN before serving its first `tomar()`, the new master MUST run catch-up recovery and
  MUST commit an `expirar` entry for that reservation
- AND the first `tomar()` served after catch-up MUST NOT return that dead pedido as if it
  were still reserved-and-alive

#### Scenario: tomar() is held until catch-up completes

- GIVEN a node that just became master and has not yet completed its one-shot recovery pass
- WHEN a worker calls `POST /pedidos/tomar` against it
- THEN the master MUST complete catch-up recovery before resolving that call, rather than
  serving directly from a log it has not yet swept for leaderless-window expiries

#### Scenario: Catch-up runs exactly once per election win

- GIVEN a master that already completed its post-election catch-up pass
- WHEN subsequent `tomar()` calls arrive
- THEN the master MUST NOT re-run the full catch-up pass for each call; catch-up is a
  one-shot step gating the first `tomar()`, not a per-request check
