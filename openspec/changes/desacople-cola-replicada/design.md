# Design: Decouple the queue into a replicated standalone service (Raft-lite master/slave)

Change: `desacople-cola-replicada`
Repositories: `sdypp_colas_serv` (queue service) and `sdypp_balanceador` (client).
Inputs: `proposal.md` (approved), `exploration.md` (verified current state),
`docs/plan-cola-desacoplada.md`, `docs/contrato-worker.md`.

Settled upstream and **not re-opened here**: hand-rolled Raft-lite, standard library only,
minimum 3 odd nodes, `421 Misdirected Request` as the wrong-node redirect, post-election
catch-up recovery in scope, repository split already done.

---

## Technical Approach

Three layers, stacked so that each one can be tested without the one below it:

```
                    ┌───────────────────────────────────────────────┐
  HTTP / sockets    │ servidor.py      routes, tokens, 421, /health │
                    ├───────────────────────────────────────────────┤
  threads + I/O     │ motor.py         MotorRaft: timers, peer HTTP │
                    │                  client, commit waiters       │
                    ├───────────────────────────────────────────────┤
  pure state machine│ raft.py          NodoRaft: terms, votes, log, │
                    │                  election, commit index       │
                    ├───────────────────────────────────────────────┤
  pure application  │ aplicar.py       Aplicador: committed entry → │
                    │                  Sistema mutation             │
                    ├───────────────────────────────────────────────┤
  business logic    │ colas.py         Sistema / ColaPedidos /      │
                    │                  ColaRespuestas (two seams)   │
                    └───────────────────────────────────────────────┘
```

The load-bearing property of the whole design is: **a committed log entry must apply to
identical state on every node and produce identical resulting state.** Everything below —
the clock decision, the `colas.py` seams, the `tomar` blocking rule — falls out of that one
requirement. Any nondeterminism (a clock read, a `uuid4()`, a "which entries expired?"
judgement) must be resolved **by the master, before the entry is appended**, and travel
inside the payload. The apply step is then a pure function of `(state, payload)`.

`raft.py` contains no threads, no sockets and no timers: time enters only as an explicit
argument, outbound messages are *returned* rather than sent. `motor.py` is the only file in
the queue service that owns a thread or a socket to a peer. This is the seam the exploration
called non-optional, and it is what makes election safety testable without a wall clock.

---

## Architecture Decisions

### Decision 1: The log's operation vocabulary is the `Sistema` method surface, and the apply step is a new `Aplicador` seam

**Choice.** Committed entries are applied by a new module `sdypp_colas_serv/aplicar.py`
exposing `Aplicador(sistema).aplicar(entrada) -> resultado`. Each of the six operations maps
to **exactly one** `Sistema`-level call, never to the lower-level `ColaPedidos` /
`ColaRespuestas` primitives:

| `operacion` | Apply step invokes | Mutations performed |
|---|---|---|
| `encolar` | `Sistema.publicar_pedido(Pedido(**payload))` | 1 (`_esperando.append`) |
| `tomar` | `Sistema.reservar_pedido(id, consumidor, reservado_hasta_ms)` | 1 (`_esperando` → `_en_vuelo`) |
| `devolver` | `Sistema.devolver_pedido(id, consumidor)` | 1 (`_en_vuelo` → `_esperando`) |
| `responder` | `Sistema.responder(id, estado, contenido, atendido_por, app, espera_ms)` | **2** (pop pedido + publish respuesta) |
| `retirar-respuesta` | `Sistema.retirar_respuesta(destinatario)` | 1 (`popleft`) |
| `expirar` | `Sistema.expirar(decision)` | **2..2N** (fail/requeue pedidos + purge respuestas) |

The plan treats `responder` as one log entry while `Sistema.responder()` (`colas.py:385-408`)
performs two mutations. That is not a contradiction to resolve — it is the reason the apply
step must call `Sistema`, not `ColaPedidos`. **`Sistema` is already the compound-atomicity
boundary of this codebase** (its own docstring, `colas.py:359-368`, says so: "las tres
operaciones interesantes tocan las dos [colas]" and it exists to fix lock ordering). Applying
one committed entry through one `Sistema` call is therefore atomic-on-replay for free: a
replaying slave runs the identical two mutations in the identical order, and there is no
intermediate state any reader can observe, because the apply step holds both condition locks
for the duration (see Decision 8).

The only failure mode left is a *partial* apply — mutation one succeeds, mutation two raises.
`Sistema.responder()` already returns `(False, "destinatario-saturado")` **after** having
popped the pedido, which is a real half-apply in today's code. Under replication that
divergence is silent and permanent. So `Sistema.responder()` gains a pre-check: saturation is
evaluated before `pedidos.completar()`, under both locks, and the method returns
`(False, "destinatario-saturado")` without mutating anything. Deterministic on every node.

**Alternatives considered.**
- *Apply step calls `ColaPedidos`/`ColaRespuestas` directly, one entry per primitive mutation*
  (so `responder` becomes two entries). Rejected: two entries can be split by a commit-index
  boundary or by a failover, so a slave can be promoted holding "pedido removed, respuesta
  never published" — a request silently lost after a `202`. That is precisely the guarantee
  this change exists to provide.
- *Apply step re-runs `servidor.py` handler logic.* Rejected: the handler validates, generates
  ids, reads clocks and writes the bitácora. All four are nondeterministic or side-effecting.

### Decision 2: `colas.py` gets two surgical seams — an injected clock, and expiry split into decide/apply

**Choice.** The proposal says "`colas.py` business logic is unchanged; what changes is *when*
it is invoked." That is true of the business rules and false of the module's surface. Two
seams are unavoidable, and both are small:

**Seam A — injected clock.** `Sistema`, `ColaPedidos`, `ColaRespuestas` and `Pedido` take a
`reloj` callable (default `time.monotonic`, see Decision 3 for what is actually passed).
`Pedido.encolado_en` loses its `default_factory=time.monotonic` (`colas.py:60`) and becomes a
required field supplied from the payload. Without this, two nodes applying the same
`encolar` entry produce pedidos with different `encolado_en`, and therefore different
`esperaMs` in the eventual response — observable divergence in a contract field.

**Seam B — expiry splits into a pure decision and a pure application.**

```python
# decide (master only, reads the clock, mutates nothing)
ColaPedidos.detectar_vencidos(ahora) -> DecisionExpiry
ColaRespuestas.detectar_purgables(corte) -> {destinatario: cantidad}

# apply (every node, deterministic, takes the decision verbatim)
ColaPedidos.aplicar_expiry(decision) -> [(pedido, estado, detalle)]
ColaRespuestas.aplicar_purga(purga) -> int
```

`DecisionExpiry` is a plain dict carrying explicit id lists, never predicates:

```python
{"reencolar":  ["id1", ...],                      # in-flight, idempotent, budget left
 "fallar":     [{"id": "id2", "estado": "DEADLINE_EXCEEDED", "detalle": "..."}, ...],
 "purgar":     {"balanceador@casa-tomas": 3},     # how many to drop per destinatario
 "decididoEnMs": 1758326400123}
```

`Sistema.recuperar()` (`colas.py:410-424`) splits the same way into `Sistema.decidir_expiry()`
and `Sistema.expirar(decision)`; only the latter is reachable from the apply step.
`_proximo_vivo()` (`colas.py:267-276`) is replaced by the non-destructive `_frente_vivo()`
described in Decision 5 — it no longer pops and no longer feeds `_a_fallar`. `_a_fallar`
disappears entirely: it existed only to shuttle expiry decisions from the `tomar` path to the
recoverer thread, and expiry decisions now travel through the log instead.

Everything else in `colas.py` — the retry rule, the append-left semantics, the late-answer
branch of `completar()`, the cota checks, the reserva computation — is untouched. The existing
`tests/test_colas.py` is relocated and keeps passing, with additions only for the new methods.

**Alternatives considered.**
- *Leave `colas.py` untouched, wrap it.* Rejected as impossible, not merely worse: no wrapper
  can make `Pedido.encolado_en`'s `default_factory` deterministic, and no wrapper can stop
  `ColaPedidos.recuperar()` from reading `time.monotonic()` inside itself (`colas.py:212`).
- *Replicate the clock reading instead of the decision* (each node runs `recuperar()` with a
  replicated `ahora`). Rejected: the plan explicitly wants the *decision* replicated, and this
  variant still diverges, because whether a pedido is in `_en_vuelo` at that instant depends on
  the local apply position, not only on the timestamp.

### Decision 3: Deadlines become absolute wall-clock milliseconds stamped by the master

**Choice.** `Pedido.vence_en` and `Pedido.reservado_hasta` stop being `time.monotonic()`
values and become absolute epoch milliseconds (`venceEnMs`, `reservadoHastaMs`) stamped by
the master from `time.time()` at the moment it appends the entry. Every node stores the value
verbatim. `queda()` becomes `(self.vence_en_ms - self.reloj_ms()) / 1000`.

`time.monotonic()` survives for everything strictly node-local and non-replicated: long-poll
deadlines, `ColaPedidos.vistos`, and `esperaMs` is derived from the replicated
`encoladoEnMs`. The wire contract is unchanged — `quedaMs` is still computed by the serving
master at the instant of serving, and no absolute instant ever reaches a worker.

**Rationale.** A monotonic value is meaningful only inside the process that produced it. A
slave storing the master's `vence_en` verbatim holds a number from a foreign epoch; if it is
promoted, every deadline in its queue is wrong by an unbounded amount. The original reason
for choosing monotonic (`colas.py:47-50`) was to keep *four houses'* desynchronised clocks out
of the budget arithmetic — and that reason is fully preserved, because the budget still never
travels as an absolute instant. What changes is the much narrower exposure of three cluster
nodes disagreeing about wall time, and only at the instant of failover.

**Alternatives considered.**
- *Per-node monotonic re-anchoring* — each node stores `offset = reloj_local() - instante_del_master`
  refreshed by every heartbeat, and converts on the fly. Rejected: it is the same clock-skew
  exposure with a moving offset, and a node catching up after downtime applies a burst of
  entries against a stale offset, silently extending every deadline by its downtime. Strictly
  more machinery for strictly worse behaviour.
- *A logical cluster clock (deadlines as log indices).* Rejected: budgets are wall-time
  commitments to a human-facing client (`presupuestoMs`), and a log-index clock stops
  advancing exactly when the cluster is idle — the moment expiry matters most.
- *Ship NTP synchronisation as a dependency.* Out of scope; the tailnet already runs NTP and
  the residual skew is bounded by `COLA_TOLERANCIA_RELOJ` (see Migration).

### Decision 4: `NodoRaft` is a pure state machine; time is an argument and messages are return values

**Choice.** `raft.py` exposes one class with no imports beyond `dataclasses` and typing — no
`threading`, no `time`, no `http`, no `random` module-level use:

```python
class NodoRaft:
    def __init__(self, yo, pares, reloj, azar, timeout_eleccion_ms, heartbeat_ms): ...

    # driven by the outside world; every one returns outbound messages, sends nothing
    def tic(self, ahora_ms)                      -> list[Mensaje]
    def recibir_solicitud_voto(self, msg)        -> (Respuesta, list[Mensaje])
    def recibir_respuesta_voto(self, de, msg)    -> list[Mensaje]
    def recibir_append(self, msg)                -> (Respuesta, list[Mensaje])
    def recibir_respuesta_append(self, de, msg)  -> list[Mensaje]
    def proponer(self, operacion, payload)       -> (indice, termino) | None   # master only
    def entradas_a_aplicar(self)                 -> list[Entrada]              # advances indiceAplicado
    def instantanea(self)                        -> dict                        # for /raft/estado and /health
```

`reloj` is a zero-argument callable returning integer milliseconds. `azar` is a
`random.Random` instance, injected — so election jitter is `azar.randint(...)` on a seeded
generator, reproducible byte for byte. `tic(ahora_ms)` is the *only* thing that can start an
election or emit a heartbeat, and it is called by `motor.py` from a thread in production and
by a test loop in `unittest`. There is no `threading.Timer` anywhere in `raft.py`.

The commit index is advanced inside `recibir_respuesta_append`; applying is pulled by the
caller via `entradas_a_aplicar()` rather than pushed through a callback, so a test can inspect
`indiceCommit` without a state machine attached at all.

**Alternatives considered.**
- *`NodoRaft` owns its own thread and a `transporte` collaborator.* Rejected: every test then
  needs a fake transport *and* real elapsed time, which is exactly the flakiness the
  exploration flagged as the top risk. Returning messages makes "deliver nothing" a first-class
  test operation, which is how partitions are simulated.
- *Callback-on-commit (`al_comprometer=fn`).* Rejected: it inverts control, so a test asserting
  on commit-index advancement has to assert on a spy's call log rather than on a value.

### Decision 5: `tomar()` blocks until the `expirar` entry commits — implemented as propose-and-wait outside the queue lock

**Choice.** Option (b) from the exploration, as the proposal settled. The exact mechanism:

`ColaPedidos.tomar()` as it exists today (`colas.py:127-149`) is **removed from the serving
path**. It is replaced by two pieces:

1. `ColaPedidos.inspeccionar_frente(ahora_ms, excluidos)` — a pure read under `_hay`,
   mutating nothing, returning one of:
   - `("vacio", None)` — `_esperando` is empty (or every candidate is in `excluidos`);
   - `("vencidos", [ids])` — the head of the queue is one or more dead pedidos;
   - `("vivo", pedido_id)` — the first live pedido not already spoken for.
2. `ColaPedidos.esperar_cambio(timeout)` — `with self._hay: self._hay.wait(timeout)`, the
   existing long-poll wait, unchanged in spirit.

The serving handler then runs this loop (in `servidor.py`, master only):

```
limite = ahora + espera
while ahora < limite:
    clase, dato = sistema.inspeccionar_frente(ahora_ms, motor.ids_propuestos())
    if clase == "vacio":
        sistema.esperar_cambio(min(limite - ahora, LATIDO)); continue
    if clase == "vencidos":
        decision = sistema.decidir_expiry(ahora_ms)          # covers those ids and more
        motor.proponer_y_esperar("expirar", decision, limite) # ← THE BLOCK
        continue                                              # re-inspect from scratch
    # clase == "vivo"
    payload = {"id": dato, "consumidor": c,
               "reservadoHastaMs": min(ahora_ms + reserva_ms, pedido.vence_en_ms)}
    if motor.proponer_y_esperar("tomar", payload, limite) is COMPROMETIDO:
        pedido = sistema.pedido_en_vuelo(dato)
        if pedido is not None: return 200, pedido.como_json()
    continue                                                  # lost the race, try again
return 204
```

Precise behaviour of the block:

- **Scope.** Only the `"vencidos"` branch blocks on an expiry commit, and only for the calls
  that actually meet a dead head-of-queue item. A `tomar` against a healthy queue pays exactly
  one commit round-trip (the `tomar` entry itself), which it had to pay anyway under Rule 2.
- **Bound.** `proponer_y_esperar` never waits past the caller's long-poll `limite`. On timeout
  the handler returns `204` ("no work right now") — never `500`, and never the dead pedido.
  `204` is already a legal, expected answer on this route, so no client changes.
- **Loss of mastership mid-wait.** `proponer_y_esperar` returns `DESTITUIDO` the moment
  `NodoRaft` steps down (Rule 3); the handler answers `421` with the newly known master. The
  uncommitted entry is simply never applied — correct, since it was never acknowledged.
- **Nothing is popped before commit.** This is the substantive change versus today:
  `_proximo_vivo()` popped destructively *during inspection* (`colas.py:272`), so a crash
  between pop and reserve lost the pedido. Inspection is now read-only and the pop happens
  only in the apply step of a committed entry.

**Rationale.** Option (a) — serving a locally-detected-but-uncommitted expiry as if alive —
hands a worker a pedido the master already knows is dead. That burns a worker slot, produces a
`DEADLINE_EXCEEDED` the client waits the full budget for, and directly contradicts the stated
reason `_proximo_vivo()` exists at all ("entregarle a un worker un pedido que ya venció es
gastarle una réplica al pedo", `colas.py:268-270`). The cost of option (b) is latency on a
path whose contract is *already* "hang for up to `espera` seconds" — the caller cannot tell a
commit round-trip from a quiet queue. We are spending a budget the caller already granted.

**Alternatives considered.**
- *Option (a) with an "expired" flag on the response.* Rejected: invents a wire-contract field
  to paper over a correctness hole, and `docs/contrato-worker.md` is frozen and already
  delivered.
- *Speculative apply, rolled back if the entry fails to commit.* Rejected: rollback of a
  compound mutation is exactly the half-apply hazard Decision 1 removes. No rollback anywhere
  in this design.

### Decision 6: Duplicate-`tomar` prevention via a master-local `ids_propuestos()` set

**Choice.** Between inspection and commit, a second concurrent `tomar` could inspect the same
head pedido and propose a second `tomar` entry for it. `MotorRaft` therefore keeps
`_propuestos: set[str]` — pedido ids named by entries appended but not yet applied — added
under the proposal lock at append and removed at apply. `inspeccionar_frente` skips them.

This is an *optimisation for the common case*, not the correctness mechanism. The correctness
mechanism is that **the apply step is tolerant**: `Sistema.reservar_pedido(id, ...)` returns
`None` if `id` is no longer in `_esperando` (already reserved, already answered, already
expired), and the apply step treats that as a legitimate no-op. No apply step ever raises or
asserts on a stale target — a log entry that has become irrelevant by the time it commits is
a normal outcome, and an exception there would diverge the node from its peers. The handler
sees `COMPROMETIDO` but `pedido_en_vuelo(id) is None`, and simply loops.

The same tolerance rule applies to `devolver` (id not in `_en_vuelo` → no-op),
`responder` (unknown id → the existing `(False, "desconocido")` path), `retirar-respuesta`
(empty deque → no-op) and `expirar` (ids already gone → skipped).

### Decision 7: Post-election catch-up is a two-phase barrier, and the second phase is a normal log entry

**Choice.** A newly elected master runs `recuperacion_post_eleccion()` before its first
`tomar`, `encolar`, `responder` or `retirar-respuesta` is served. Data routes answer
`503 {"error":"recuperando"}` — not `421`, since this node *is* the master — for the duration,
which is bounded by one commit round-trip.

1. **Phase 1 — commit the inherited tail (the Raft no-op barrier).** A new master may hold
   entries from previous terms that it cannot commit directly (Raft §5.4.2 forbids committing
   a prior-term entry by counting replicas). It therefore appends one `sentinela` entry in its
   own term and waits for it to commit; committing it commits everything before it. Until that
   lands, the master does not know its own applied state, so it cannot honestly decide anything.
   `sentinela` applies as a no-op — it exists purely as a commit barrier.
2. **Phase 2 — one `expirar` sweep before serving.** With state now current, the master runs
   `decidir_expiry(ahora_ms)` and proposes it as an ordinary `expirar` entry. Reservations held
   by workers that died during the leaderless window, and pedidos whose budget ran out while no
   master existed, are failed or re-queued here — *before* any worker can be handed one.
3. Only then does the master start its heartbeat-driven `recuperador` loop (Decision 9) and
   open the data routes.

This closes exploration gap 4. Note that phase 2 needs no new machinery: it is the periodic
sweep, run once, eagerly. That is why this is cheap enough to be non-optional.

### Decision 8: Lock ordering — the log never holds a queue lock, and the apply step is the only writer

**Choice.** Four locks exist, and they are acquired in exactly this order, top to bottom,
never upward:

| # | Lock | Owner | Held for |
|---|---|---|---|
| 1 | `MotorRaft._lock_propuesta` | `motor.py` | appending an entry + registering a commit waiter. Microseconds. Never held across I/O or across a queue lock. |
| 2 | `NodoRaft` (no lock) | — | the state machine is single-threaded by construction: every entry point is called under (1) or from the single `tic` thread. |
| 3 | `ColaPedidos._hay` | `colas.py:100` | unchanged semantics |
| 4 | `ColaRespuestas._hay` | `colas.py:298` | unchanged semantics; `Sistema` already fixes 3-before-4 (`colas.py:365-367`) |

Rules that keep it deadlock-free:

- **A queue lock is never held while waiting for a commit.** `proponer_y_esperar` is called
  from the handler with no queue lock held; the handler released it when
  `inspeccionar_frente` returned. This is why inspection is a separate, short, read-only call
  rather than a callback inside `tomar()`.
- **The apply step runs on one dedicated thread** (the *applier*), which takes locks 3 and 4
  only — never lock 1. It is the single writer to `Sistema`, on every node including the
  master, which is what makes replay order the *only* order.
- **`_hay.notify()` fires from the apply step**, as it does today from `publicar()` /
  `devolver()` / `recuperar()`. Long-poll waiters therefore wake on *committed* state only, and
  every existing waiter (`ColaPedidos.tomar`'s wait, `ColaRespuestas.tomar`'s wait) keeps
  working with no change to its wait/notify protocol.
- **Commit waiters wait on their own `threading.Event`**, one per pending entry index, keyed
  in a dict under lock 1. The applier sets them after applying, never while holding lock 1.
- The `LATIDO = 0.5` cap on every `wait()` (`colas.py:34`) is retained, so even a lost notify
  degrades to 500 ms of latency, never to a hang.

### Decision 9: Single-node mode is a majority-of-one, not a special case

**Choice.** No `if len(pares) == 1` branches in the data path. A node whose peer set is empty
computes `mayoria = 1`, so:

- its first `tic()` starts an election, it votes for itself, and `1 >= 1` elects it master
  immediately (bounded by one election timeout, which is why `RAFT_ELECCION_TIMEOUT_MS`
  defaults low enough that startup is not user-visible);
- `proponer_y_esperar` commits synchronously, because `indiceCoincidente[yo]` alone is a
  majority;
- it is always master, so `421` is unreachable by construction, not by a flag;
- `recuperacion_post_eleccion` runs once at startup and is a no-op against an empty queue.

`test_servidor_cola.py` therefore passes **unchanged** against a one-node `COLA_PARES`, which
is the regression guard the proposal asks for. `BA_COLA_URL` with one entry is a one-element
seed list, and `ClienteReplica` never has anything to redirect to.

### Decision 10: `ClienteReplica` layers redirect-following *above* the unchanged `_pedir` retry

**Choice.** `ClienteReplica` owns a `dict[url] -> ClienteCola`, one pooled client per seed
node, and `clientecola.py` is **not** rewritten — `ErrorCola`, the connection pool, and the
stale-connection-only retry (`clientecola.py:87-132`) are kept verbatim as the transport
layer. The new module is strictly a routing layer on top. `clientecola.py` is retained, not
removed; the proposal's "Modified/Removed" resolves to *retained as transport*.

That layering is the whole point: `_pedir` already encodes "retry only a reused connection,
because a fresh-connection failure has unknown delivery status", and a redirect layer that
re-implemented transport would lose it.

### Decision 11: Slave catch-up uses a conflict-term hint, not one-index-at-a-time backtracking

**Choice.** On `appendEntries` rejection the follower returns
`{"exito": false, "terminoConflicto": t, "primerIndiceDelTermino": i}`, and the master sets
`siguienteIndice[par] = i` rather than decrementing by one. A slave that was down for a
thousand entries resynchronises in a couple of round trips instead of a thousand heartbeats.
This is Raft's standard optimisation and it costs about ten lines; without it, chaos scenario
5 ("levantar de nuevo un slave caído") takes visibly long enough to look broken in the demo.

---

## Data Flow

### `POST /pedidos` against a healthy 3-node cluster

```
balanceador          master                    slave-1        slave-2
    │                  │                          │              │
    ├─ POST /pedidos ──▶                          │              │
    │                  ├ validate, stamp id +     │              │
    │                  │ venceEnMs + encoladoEnMs │              │
    │                  ├ NodoRaft.proponer("encolar", payload)   │
    │                  ├─ /raft/appendEntries ────▶              │
    │                  ├─ /raft/appendEntries ───────────────────▶
    │                  ◀──────── exito, indice ───┤              │
    │                  ├ majority (2/3) → indiceCommit = N       │
    │                  ├ applier: Sistema.publicar_pedido(...)   │
    │                  │   └ _hay.notify() wakes a parked tomar  │
    ◀── 202 {id} ──────┤                          │              │
    │                  ◀──────── exito (late) ────────────────────┤
    │                  │                     (slaves apply on next heartbeat's indiceCommit)
```

### `POST /pedidos/tomar` meeting an expired head-of-queue item

```
worker                master
   ├─ POST /tomar ────▶
   │                   ├ inspeccionar_frente → ("vencidos", [id7])
   │                   ├ decidir_expiry(ahora_ms) → {fallar:[id7], reencolar:[], purgar:{}}
   │                   ├ propose "expirar" ──▶ majority ──▶ commit
   │                   ├ applier: Sistema.expirar(...)  → respuesta DEADLINE_EXCEEDED
   │                   │          for id7's destinatario, published to ColaRespuestas
   │                   ├ loop: inspeccionar_frente → ("vivo", id8)
   │                   ├ propose "tomar" {id8,...} ──▶ majority ──▶ commit
   │                   ├ applier: Sistema.reservar_pedido(id8, ...)
   ◀── 200 {id8} ──────┤
```

### Master failover, from a client's point of view

```
ClienteReplica          old master (dies)   slave-1 → new master   slave-2
   ├─ POST ────────────▶ ✗ connection reset
   ├ ErrorCola(enviado=?) → drop cached master, probe seed list
   ├─ GET /health ─────────────────────────▶ {"rol":"candidato","masterConocido":null}
   ├ backoff (100ms, 200ms, 400ms … capped)
   │                                       ├ wins election, sentinela commits,
   │                                       ├ expirar sweep commits
   ├─ GET /health ─────────────────────────▶ {"rol":"master","termino":4}
   ├ cache master = slave-1
   ├─ POST /pedidos ───────────────────────▶ 202
```

---

## File Changes

### `sdypp_colas_serv`

| File | Action | Description |
|---|---|---|
| `raft.py` | Create | `NodoRaft` pure state machine: terms, `voto_para`, log, election with the "at least as up to date" rule, `appendEntries` with log matching + conflict-term hint, commit-index advancement with the same-term restriction, term fencing. Injected `reloj` and `azar`. No threads, sockets or timers. |
| `motor.py` | Create | `MotorRaft`: the tic thread, the peer HTTP client (reusing a pooled `http.client` in the shape of `clientecola.py`), `proponer_y_esperar`, the applier thread, `ids_propuestos()`, `recuperacion_post_eleccion()`, the master-gated recoverer loop. The only file here that owns a thread or a peer socket. |
| `aplicar.py` | Create | `Aplicador`: the six-operation dispatch table of Decision 1 plus `sentinela`. Pure; takes a `Sistema` and an entry, returns a result. Unknown `operacion` → logged and skipped (forward compatibility), never raised. |
| `colas.py` | Modify | Seams A and B (Decision 2); `vence_en`/`reservado_hasta` → `*_ms` absolute (Decision 3); `tomar()`→`inspeccionar_frente()`+`esperar_cambio()`; `reservar_pedido`, `retirar_respuesta`, `pedido_en_vuelo` added; `responder()` saturation pre-check; `_a_fallar` and `_proximo_vivo()` removed. Retry/FIFO/append-left/late-answer rules untouched. |
| `servidor.py` | Modify | Role/term gate on every data route → `421 {"error":"no-soy-master","master":...}`; `503 {"error":"recuperando"}` during post-election catch-up; `/raft/appendEntries`, `/raft/requestVote`, `/raft/estado`; three-token split; `/health` gains `rol`/`termino`/`masterConocido`/`instancia`/`contrato`; `/health/vivo` added; `tomar_pedido` becomes the propose-and-wait loop; `recuperador()` moves into `motor.py` and is master-gated. |
| `README.md` | Modify | Raft-lite protocol, the four correctness rules, "no proxy in front of the cluster", the wall-clock decision and its skew bound. |
| `tests/__init__.py`, `tests/test_raft.py` | Create | Pure state-machine tests (see Testing Strategy). |
| `tests/test_aplicar.py` | Create | Determinism and tolerance of the apply step. |
| `tests/test_cluster_raft.py` | Create | In-process 3-node `ThreadingHTTPServer` integration. |
| `tests/test_colas.py`, `tests/test_servidor_cola.py` | Create (relocated) | Moved from `sdypp_balanceador/tests/`. |
| `openspec/config.yaml` (or equivalent) | Modify | Record `test_command: python3 -m unittest discover -s tests`. |

### `sdypp_balanceador`

| File | Action | Description |
|---|---|---|
| `docs/contrato-worker.md` | Modify | `421` replaces `409 no-soy-master`; collision box → plain table; pseudocode branch → `codigo == 421`. **Slice 1, ships first.** |
| `docs/plan-cola-desacoplada.md` | Modify | Same substitution in Regla 1, the discovery table, the contract section and every route. |
| `app/clientereplica.py` | Create | `ClienteReplica` (see Interfaces). |
| `app/clientecola.py` | Modify | Retained as the transport layer. `ErrorCola` gains an explicit `enviado` attribute (today the distinction is implicit in which branch raises). `tomar_respuesta()` stops collapsing non-200 into `None` — it returns `(codigo, datos)` and `ClienteReplica` decides. |
| `app/balanceador.py` | Modify | `BA_COLA_URL` comma-split (`:73`), `ClienteReplica(URLS, ...)` (`:288`), `derivar()` never sees a redirect (`:386-441`), `recolectar()` distinguishes 204 / 503 / 421 / `ErrorCola` (`:346-376`), `salud()` gains `cola.rol`/`termino`/`estado`/`instancias` (`:550-604`). |
| `consola.py` | Modify | Generate a comma-separated seed list (`:100`, `:641`). |
| `tests/test_cluster_cola.py` | Create | Fake cluster of scripted `http.server` nodes. |
| `tests/test_colas.py`, `tests/test_servidor_cola.py` | Delete | Relocated to `sdypp_colas_serv`. |
| `cola/` | Delete | Verified byte-identical duplicate; removed last, in slice 6. |

---

## Interfaces / Contracts

### Log entry

```python
@dataclass(frozen=True)
class Entrada:
    indice: int          # 1-based, contiguous, assigned by the master at append
    termino: int         # the master's term at append; never rewritten
    operacion: str       # encolar | tomar | devolver | responder |
                         # retirar-respuesta | expirar | sentinela
    payload: dict        # fully resolved: no clock reads, no uuid4, no predicates
```

Payload shapes — every nondeterministic value is pre-resolved by the master:

```python
"encolar":           {"id", "operacion", "parametros", "idempotente", "destinatario",
                      "cliente", "venceEnMs", "encoladoEnMs"}
"tomar":             {"id", "consumidor", "reservadoHastaMs"}
"devolver":          {"id", "consumidor"}
"responder":         {"id", "estado", "contenido", "atendidoPor", "app", "resueltoEnMs"}
"retirar-respuesta": {"destinatario"}
"expirar":           {"reencolar": [id], "fallar": [{"id","estado","detalle"}],
                      "purgar": {destinatario: n}, "decididoEnMs"}
"sentinela":         {}
```

### `/raft/appendEntries` (POST, `X-Cola-Token` = `COLA_TOKEN_CLUSTER`)

```jsonc
// →
{"termino": 4, "master": "http://cola-2:8085",
 "indicePrevio": 12, "terminoPrevio": 3,        // log-matching anchor
 "entradas": [ {"indice":13,"termino":4,"operacion":"encolar","payload":{…}} ],
 "indiceCommit": 12}
// ← 200
{"termino": 4, "exito": true,  "indiceCoincidente": 13}
{"termino": 4, "exito": false, "terminoConflicto": 3, "primerIndiceDelTermino": 9}
// ← 403 token inválido
```

### `/raft/requestVote` (POST, cluster token)

```jsonc
// →  {"termino": 5, "candidato": "http://cola-3:8085",
//     "ultimoIndiceLog": 13, "ultimoTerminoLog": 4}
// ← 200 {"termino": 5, "votoConcedido": true}
```

Vote granted iff: `msg.termino >= termino_actual`, and `voto_para in (None, candidato)` for
that term, and the candidate's log is at least as up to date — last term first, index on a tie.

### Commit-index advancement rule

```python
def avanzar_commit(self):
    """Raft §5.4.2: a master commits ONLY entries from its own term by counting
    replicas. Prior-term entries ride along once a same-term entry commits."""
    for n in range(self.indice_ultimo, self.indice_commit, -1):
        if self.log[n].termino != self.termino_actual:
            continue                 # NOT `break`: an older entry may still ride along
        replicas = 1 + sum(1 for p in self.pares if self.indice_coincidente[p] >= n)
        if replicas >= self.mayoria:
            self.indice_commit = n
            return
```

`mayoria = (len(pares) + 1) // 2 + 1`, counting the master. With one node: `1`.
Applying is separate: `entradas_a_aplicar()` returns
`log[indice_aplicado+1 : indice_commit+1]` and advances `indice_aplicado`.

### Term fencing (Rule 3), applied uniformly at every message boundary

```python
def _fencing(self, termino_ajeno):
    if termino_ajeno > self.termino_actual:
        self.termino_actual = termino_ajeno
        self.voto_para = None
        self.rol = "slave"
        self.master_conocido = None
        return True     # stepped down
    return False
```

Called as the first statement of all four `recibir_*` methods. A master that steps down mid-wait
causes every pending `proponer_y_esperar` to return `DESTITUIDO` (Decision 5).

### `ClienteReplica`

```python
class ClienteReplica:
    def __init__(self, urls, token="", timeout=5.0, conexiones=8,
                 backoff_inicial=0.1, backoff_maximo=1.0, presupuesto=5.0): ...

    # same surface the balanceador already consumes
    def publicar_pedido(self, pedido)                  -> (codigo, datos)
    def tomar_respuesta(self, destinatario, espera)    -> (codigo, datos)   # widened
    def estado(self)                                   -> dict | None
    def master_conocido(self)                          -> str | None
    def instancias(self)                               -> [{"url","instancia","rol","termino"}]
    def cerrar(self)                                   -> None
```

Internals, precisely:

- **Seed list.** `urls` is the static node list; one `ClienteCola` per url, created lazily and
  cached, each with its own connection pool. Never mutated at runtime (no dynamic membership).
- **Master cache.** `_master: str | None` guarded by a `threading.Lock`. Every successful
  non-`421` response confirms it; `None` means "rediscover".
- **Discovery.** Cold start, or `_master is None`: `GET /health` sequentially over the seed
  list (starting at a rotating offset so N clients do not all hammer node 0), taking the first
  `rol == "master"`. A `rol == "slave"` answer with a non-null `masterConocido` is a usable
  shortcut and is taken immediately. Zero masters found → leaderless.
- **Redirect, exactly once, never duplicating a POST.** A `421` is a *definitive* statement
  that the node did not process the request — it is emitted by the role gate before any
  mutation, before anything is proposed. That is what makes re-sending safe, and the reason it
  must be a distinct status: an `ErrorCola` from a fresh connection is *not* safe to re-send
  and is never re-sent. The follow is one hop only; a second `421` drops the cache and falls
  back to discovery + backoff, so a redirect cycle cannot loop.
- **`ErrorCola.enviado`.** Preserved verbatim from `clientecola.py:87-132`: a stale reused
  connection is retried inside `_pedir` (unchanged), a fresh-connection failure raises and
  `ClienteReplica` responds by invalidating the master cache and re-probing — it does **not**
  re-send the POST. `publicar_pedido` propagates `ErrorCola` to `derivar()`, which already maps
  it to `503` (`balanceador.py:412-415`).
- **Backoff while leaderless.** `0.1s`, doubling to a `1.0s` cap, with `±20%` jitter, until the
  caller's budget is spent; then `(503, {"error": "el sistema de colas no responde"})`.
  `recolectar()` gets the same treatment via the widened `tomar_respuesta`, which is what keeps
  balanceador CPU near zero against a dead cluster.
- **Never writes to a slave.** Every data call goes to the cached master or to a node that just
  reported `rol == "master"`; a `421` response is never treated as a result.

### `recolectar()` status handling (`balanceador.py:346-376`)

| Outcome | Today | After |
|---|---|---|
| `200` | dispatch | dispatch (unchanged) |
| `204` | `continue`, no sleep | `continue`, no sleep — an empty long-poll is normal |
| `503` | indistinguishable from 204 → hot loop | `time.sleep(ESPERA_REINTENTO)` then continue |
| `421` | indistinguishable from 204 → hot loop against a slave | handled *inside* `ClienteReplica`; never reaches `recolectar()` |
| `ErrorCola` | sleep (already correct) | unchanged |

### `/health` on a queue node (additive)

```jsonc
{"cola": "sana", "casa": "casa-tomas", "arrancado": "…",
 "instancia": "cola-8085@casa-tomas", "contrato": "1.0",
 "rol": "master", "termino": 4, "masterConocido": "http://cola-2:8085",
 "indiceLog": 13, "indiceCommit": 13, "recuperando": false,
 "esperando": 2, "enVuelo": 1, "cota": 100, "respuestasPendientes": 0}
```

`GET /health/vivo` → `200 {"vivo": true}`, unauthenticated, no state inspection, for the
container `HEALTHCHECK`. It answers `200` even while `recuperando` is true — the process is
alive and must not be restarted mid-election.

---

## Testing Strategy

Strict TDD: every row below is written RED first. The backbone is the `ClusterFalso` harness
in `tests/test_raft.py`, which makes election timing a *choice* rather than a race.

```python
class ClusterFalso:
    """N NodoRaft over an in-memory bus. No threads, no sockets, no real time."""
    def __init__(self, n, semilla=1234): ...
    def avanzar(self, ms, nodos=None)      # step the fake clock, call tic() on each
    def entregar(self, veces=None)         # deliver queued messages, honouring the partition
    def particionar(self, grupo_a, grupo_b)  # drop every message across the cut
    def sanar(self)
    def forzar_eleccion(self, nodo)        # advance ONLY that node past its timeout
    def estabilizar(self, ms=2000)         # avanzar+entregar until quiescent
    def masters(self)                      # -> [ids] ; asserting len<=1 is the safety check
```

| Layer | What to test | Approach |
|---|---|---|
| Unit — `raft.py` | Majority election at 3 and 5 nodes; a stale-log candidate loses (older last-term, and same-term-lower-index); a node seeing a higher term steps down from any role; a node votes at most once per term; `appendEntries` rejects on a log-matching mismatch and returns the conflict hint; the master backs up to `primerIndiceDelTermino` and converges; commit index never advances on a prior-term entry alone, and does advance transitively once a same-term entry commits; `mayoria == 1` for a single node. | `ClusterFalso`, deterministic `forzar_eleccion`, seeded `azar`. Zero real timers. |
| Unit — safety invariant | **No two masters, ever** — asserted after *every* `entregar()` in a randomised-but-seeded schedule of 200 steps that partitions, heals, kills and revives nodes; plus the explicit zombie case (old master revived, sees a higher term on first contact, steps down, commits nothing). | Property-style loop over a seeded schedule; reproducible from the seed printed on failure. |
| Unit — `aplicar.py` | Applying an identical entry sequence to two fresh `Sistema` instances yields byte-identical `estado()` and identical `como_json()` for every pedido; `responder` is all-or-nothing (saturated destinatario mutates nothing); every operation is a tolerant no-op against a stale target; an unknown `operacion` is skipped, not raised. | Direct construction, `RelojFalso` returning scripted ms values. |
| Unit — `colas.py` | Existing `test_colas.py` relocated and green; new: `inspeccionar_frente` mutates nothing and is idempotent; `detectar_vencidos` is a pure function of `(state, ahora_ms)`; `aplicar_expiry` applied twice is a no-op the second time; the retry rule (idempotent requeue vs non-idempotent fail) is preserved exactly. | `RelojFalso`, no HTTP. |
| Integration — queue cluster | 3 in-process `ThreadingHTTPServer` nodes on `127.0.0.1` free ports: exactly one `rol:"master"`; `202` does not return before a majority acks (one node's `/raft/appendEntries` handler delayed by a `threading.Event` the test controls — no `sleep`); kill the master via `shutdown()` with `202`-confirmed pedidos in flight and assert the new master delivers all of them; a data route on a slave returns `421`; a `/respuestas` conflict still returns `409 {"resultado":"desconocido"}` with no overlap; a revived slave catches up in ≤3 heartbeats. | The pattern `test_servidor_cola.py` already uses. Short deterministic timeouts; "delay" is an Event the test sets, never a wall-clock sleep. |
| Integration — post-election | Expire a reservation during a forced leaderless window, then assert the new master's first `tomar` never returns that pedido, and that its `DEADLINE_EXCEEDED` respuesta is queued for its destinatario before the data routes open. | Kill the master with a reservation about to lapse; poll `/health` until `recuperando` flips false. |
| Integration — single node | `COLA_PARES` with one entry: `test_servidor_cola.py` passes **unchanged**; no route ever emits `421`; `/health` reports `rol:"master"`. | Existing suite as the regression guard. |
| Integration — `ClienteReplica` | Fake cluster of scripted minimal `http.server` handlers: cold-start discovery; `421` followed exactly once with the POST body sent exactly once (the fake asserts on received-body count, which is the actual no-duplication check); `ErrorCola` from a fresh connection is **not** re-sent; all-slaves → backoff with a bounded request count in a fixed window; `/pedidos/tomar` never sent to a node reporting `rol:"slave"`; whole cluster down → `503` and near-zero request rate. | `tests/test_cluster_cola.py`. |
| Regression — balanceador | Full suite at or above its 100-test baseline; `recolectar()` sleeps on `503`/`ErrorCola` and does not sleep on `204`. | `./.venv/bin/python -m unittest discover -s tests`. |
| E2E — manual, for the informe | The plan's Verificación section: 3 nodes by hand, plus chaos rows 0–6. | Documented, run once, evidence captured. Not part of CI. |

**Flakiness controls, stated as requirements rather than hopes.** No test in
`tests/test_raft.py` may import `time` or call `sleep`. No integration test may assert that
something happened *within* a wall-clock duration; it polls a condition with a generous
ceiling instead. Election jitter comes from an injected seeded `random.Random`. Integration
timeouts are set from env (`RAFT_ELECCION_TIMEOUT_MS=150`, `RAFT_HEARTBEAT_MS=50`) so the
suite is fast without being racy.

---

## Threat Matrix

The canonical matrix covers shell/VCS/PR automation boundaries. This change has none of them —
it introduces no subprocess, no shell invocation, no git or PR automation, and no
executable-file classification. Recorded explicitly, with no tasks generated:

| Boundary | Applicability | Reason |
|---|---|---|
| Documentation-like paths | N/A | No file is read, classified or executed by path. The only documents touched (`contrato-worker.md`, `plan-cola-desacoplada.md`) are edited by hand, never interpreted. |
| Git repository selection | N/A | No `git` invocation anywhere in the change. The subtree split already happened and is not re-run. |
| Commit state | N/A | No index or worktree manipulation. |
| Push state | N/A | No push automation. |
| PR commands | N/A | No PR automation; slices are delivered by the normal flow. |

The boundaries this change *does* introduce are network routing and authorisation. They carry
real adversarial cases, so they get their own rows with mandatory RED tests:

| Boundary | Adversarial case | Design response | RED test |
|---|---|---|---|
| Cluster protocol authorisation | An unauthenticated peer calls `/raft/requestVote` and elects itself, or injects log entries via `/raft/appendEntries` | `/raft/*` requires `COLA_TOKEN_CLUSTER`, which is distinct from the publisher and consumer tokens; `403` otherwise, with no state touched before the check | `/raft/*` with the publisher token, with the consumer token, and with no token → `403`, and `/raft/estado` shows an unchanged term |
| Token split | A worker's consumer token is used to publish pedidos, or a compromised publisher token drains the queue via `/pedidos/tomar` | Per-route token class: `/pedidos` → publisher; `/pedidos/tomar`, `/pedidos/devolver`, `/respuestas` → consumer; `/respuestas/tomar` → publisher; `/raft/*` → cluster | One `403` test per route × wrong-token-class |
| Redirect following | A malicious or misconfigured node returns `421` with a `master` URL outside the seed list, steering the balanceador's traffic off-cluster | `ClienteReplica` accepts a redirect target **only if it is already in the static seed list**; an unknown target is ignored and treated as `master: null` (fall back to discovery) | `421` with `master: "http://evil:8085"` → no request is ever sent to that host; discovery runs instead |
| Redirect loop | Two nodes each `421` towards the other (transient during an election) | One hop only, then cache invalidation + backoff; hop count is not a retry budget | Scripted mutual-`421` pair → bounded request count, then `503`, no unbounded loop |
| Unauthenticated liveness | `/health/vivo` leaks queue contents | It returns `{"vivo": true}` only — no counts, no role, no term | Assert the exact response body shape |

---

## Migration / Rollout

Six slices as the proposal fixes them; ordering, standalone-ness and per-slice rollback are
already recorded there and not restated. What this design adds:

- **Slice 4 is the only breaking-on-revert slice.** Reverting it loses committed in-memory
  state, so it carries a documented drain step. Slices 2 and 5 are unreferenced new modules;
  1, 3 and 6 revert cleanly.
- **The server leads the client.** `421` is emitted in slice 3 and understood in slice 5, never
  the reverse. Between those, the balanceador still runs against a one-element seed list
  (single-node mode), where `421` is unreachable — so there is no window in which the two
  repos disagree in a way anything observes.
- **New environment variables**, all with defaults so an unchanged deployment keeps working:
  `COLA_PARES` (comma-separated peer URLs; empty ⇒ single-node mode),
  `COLA_INSTANCIA` (default `f"{NOMBRE}-{PUERTO}@{CASA}"`),
  `RAFT_ELECCION_TIMEOUT_MS` (default `1000`), `RAFT_HEARTBEAT_MS` (default `200`),
  `RAFT_JITTER_MS` (default `400`), `COLA_TOLERANCIA_RELOJ_MS` (default `1000`).
  `COLA_TOKEN` remains honoured as the fallback for all three new token variables when they
  are unset, so an existing `cola.env` keeps working unchanged.
- **Clock-skew guard (Decision 3).** Each `appendEntries` carries the master's `instanteMs`; a
  node whose own `time.time()` differs by more than `COLA_TOLERANCIA_RELOJ_MS` logs a warning
  and surfaces `"relojDesfasadoMs"` in `/health`. It does **not** refuse to participate —
  refusing would convert a clock problem into an availability outage, which is strictly worse.
- **No data migration.** The log is in memory and the queue starts empty. "Rolling upgrade" is
  not attempted: the cluster is drained and restarted, which is legitimate because the
  no-loss guarantee covers node death, not a full-cluster restart (explicitly out of scope).

---

## Open Questions

None blocking. Two items are recorded for `sdd-spec` to freeze rather than for design to decide:

- [ ] The contract version stays `1.0`. The proposal's reasoning holds — no running node has
      ever emitted `409 no-soy-master`, so nothing deployed is being redefined. `sdd-spec`
      confirms this before freezing the wire contract, as the proposal already asks.
- [ ] `COLA_RESERVA` interacts with the added commit round-trip: a reservation is now stamped
      at *append* time and the worker receives it after commit, so it loses one round-trip of
      its 2 s window. At the plan's scale (sub-millisecond LAN RTT) this is noise, but the
      spec should state whether `reservadoHastaMs` is stamped at append or at apply. This
      design stamps it at **append**, so the value is deterministic across replicas; the
      alternative (stamp at apply) would be a clock read inside the apply step, which
      Decision 1 forbids.
