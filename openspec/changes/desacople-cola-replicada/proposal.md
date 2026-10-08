# Proposal: Decouple the queue into a replicated standalone service (Raft-lite master/slave)

Change: `desacople-cola-replicada`
Repositories affected: **both** — `sdypp_balanceador` (branch `feature/desacople`) and
`sdypp_colas_serv` (branch `feature/desacople`).
Source documents: `docs/plan-cola-desacoplada.md`, `docs/contrato-worker.md`,
`openspec/changes/desacople-cola-replicada/exploration.md`.

## Intent

The queue is a separate container today, but it is not a separate *service*: there is exactly one
possible instance, and all of its state is `deque`/`dict` in one process memory guarded by a single
`threading.Condition`. If that process dies, every accepted-but-unanswered request dies with it.
The team already decided that the priority is **not losing accepted requests when the node holding
them falls**, and that this requires one logical queue replicated across a cluster — not N
independent queues.

This change makes the queue a standalone, independently deployable service backed by a cluster of
at least 3 (odd) nodes running a hand-rolled Raft-lite: one master serves all data traffic, N−1
slaves replicate its log and stand ready for promotion. A `POST /pedidos` is only acknowledged with
`202` after a majority of the cluster holds that log entry, which is what makes the guarantee
"if the client saw `202`, the request survives the master's death" true rather than aspirational.

Success looks like: killing the master with confirmed requests in flight loses none of them; exactly
one node reports `"rol": "master"` at any instant; and the balanceador and the workers re-attach to
the new master on their own, with no restart and no reconfiguration.

### Why now

The repository split already happened. `sdypp_colas_serv/{colas.py,servidor.py,Dockerfile,README.md}`
is a verified byte-identical `git subtree split` of `cola/`, so **the plan's Etapa 3 is already
done** and everything below is planned for two live repositories from day one. The `cola/` directory
in this repo is now a duplicate awaiting removal, and every day it stays is a day the two copies can
drift. Separately, `docs/contrato-worker.md` has already been handed to the team writing the worker,
so the one open wire-level decision it carries is now blocking somebody else's work.

## Scope

### In Scope

**Wire contract (both repos, external deliverable)**

1. Replace the wrong-node redirect signal `409 {"error":"no-soy-master"}` with
   **`421 Misdirected Request`**, carrying the same body shape
   (`{"error":"no-soy-master","master":"<url o null>"}`). This is a settled decision, not an open
   question. Rationale: `/respuestas` already returns `409 {"resultado":"desconocido"}` and
   `409 {"resultado":"destinatario-saturado"}`; a third meaning for `409` on the same route makes a
   naive `status == 409` check silently discard valid responses.
2. Update **`docs/contrato-worker.md`** accordingly — it is an external deliverable already in
   another team's hands, so this update must be shipped first and announced, not folded silently
   into a later slice. The "⚠ Colisión de códigos" box collapses to a plain two-row table, the
   "decisión pendiente" note is removed, and the pseudocode's `codigo == 409 and
   respuesta.get("error") == "no-soy-master"` becomes `codigo == 421`.
3. Update **`docs/plan-cola-desacoplada.md`** for the same reason (Regla 1, the master-discovery
   table, the `CONTRATO.md` section, and the `409 no-soy-master` lines of every route).

**Queue service — `sdypp_colas_serv`**

4. A stdlib `unittest` test suite and test command for this repo, which has no `tests/` today. This
   is a prerequisite for every other item here under Strict TDD, not optional tooling.
5. `raft.py` (new): per-node state machine — `termino_actual`, `voto_para`, the append-only log;
   election with majority, the "at least as up to date" vote rule, heartbeats, `appendEntries`
   replication, commit-index advancement, and term fencing. Standard library only.
6. `servidor.py`: role/term state; `/raft/appendEntries`, `/raft/requestVote`, `/raft/estado`;
   `421` on every data route when not master; the three-way token split
   (`COLA_TOKEN_PUBLICADOR` / `COLA_TOKEN_CONSUMIDOR` / `COLA_TOKEN_CLUSTER`); `/health` gaining
   `rol`, `termino`, `masterConocido`, `instancia`, `contrato`; `/health/vivo` split out for the
   container `HEALTHCHECK`.
7. An explicit **single-node "trivially always master"** mode, so a one-element seed list keeps
   working exactly as the worker contract already promises — and so the existing
   `test_servidor_cola.py` keeps passing against an unchanged HTTP surface.
8. Route every mutation through the log: `encolar`, `tomar`, `devolver`, `responder`,
   `retirar-respuesta`, `expirar` become log entries applied to `colas.py` only once committed.
   `colas.py` business logic is unchanged; what changes is *when* it is invoked. The apply step must
   be explicit about the compound mutations — `Sistema.responder()` and `Sistema.recuperar()` each
   perform two state changes and must apply atomically from one committed entry.
9. Gate the `recuperador()` thread to the master (it starts unconditionally today), and replicate
   expiry decisions through the log so slaves do not each recompute them against their own clock.
10. **Post-election catch-up recovery**: a newly elected master MUST run a one-shot recovery pass
    before serving its first `tomar()`. Reservations expire during the leaderless window, so without
    this a fresh master hands out an already-dead pedido. This is a correctness hole absent from the
    plan, and it is in scope.

**Balanceador — `sdypp_balanceador`**

11. `app/clientereplica.py` (new): `ClienteReplica(urls, token, timeout, conexiones)` — cached
    master, cold-start `/health` probe across the seed list, one-shot `421` redirect without
    duplicating the POST, backoff while the cluster is leaderless, and `503` past the budget.
12. Make `BA_COLA_URL` accept a comma-separated seed list. This is **new work**: `balanceador.py:73`
    reads a single value and passes it to `ClienteCola(COLA_URL, COLA_TOKEN)` (`:288`), and
    `consola.py:100,641` generate a single URL. The plan's claim that it "sigue aceptando una lista
    separada por comas" is false.
13. `balanceador.py`: `derivar()` must not let a redirect reach it (today anything but 202/503 falls
    into a generic `502`); `recolectar()` must distinguish `204` from `503`/`421`/`ErrorCola` —
    `ClienteCola.tomar_respuesta()` collapses every non-200 into `None` (`:46-56`) today, which
    would turn a leaderless cluster into a closed retry loop; `salud()` gains additive
    `cola.rol` / `cola.termino` / `cola.estado` / `cola.instancias`.
14. Tests: `tests/test_cluster_cola.py` (fake cluster of minimal `http.server` nodes) and
    `sdypp_colas_serv/tests/test_raft.py`.
15. Delete `cola/` from this repo and relocate `tests/test_colas.py` / `tests/test_servidor_cola.py`
    to `sdypp_colas_serv`, closing the duplicate now that the split is verified.

### Out of Scope

- **The worker implementation itself.** Another team owns it; `docs/contrato-worker.md` is the
  interface and the only thing we owe them.
- **Etapa 4 — the queue's own `consola.py` and cluster deployment tooling.** Deferred to a follow-up
  change; the manual 3-node launch from the plan's Verificación section is sufficient to prove this
  one.
- **The vendored-client mechanism** (`cliente.py` published from the queue repo with a
  `tests/test_cliente_vendoreado.py` version check). `ClienteReplica` is written in the balanceador
  for now; vendoring is a process concern that can follow once the client's shape has settled.
- **Log persistence to disk.** The Raft log is in memory. This change survives *a node* dying, not
  the whole cluster restarting. Saying otherwise would overclaim.
- **Dynamic cluster membership.** The node set is static configuration. Changing 3→5 nodes is a
  redeploy, which sidesteps the disjoint-majority hazard the plan already documents.
- **Any external dependency** — no RabbitMQ, Kafka, Redis, or consensus library, and no
  `pip install` in the queue image. Standard library only, by constraint.
- **Re-opening Raft-lite as the approach.** Settled by the team; the rejected alternatives
  (synchronous mirror, single node plus checkpoint) are recorded in the exploration.

## Capabilities

`openspec/specs/` is empty — this is the first change in this workspace — so every capability below
is new and none are modified.

### New Capabilities

- `queue-replication`: the Raft-lite consensus state machine — terms, voting, the log, election
  safety, heartbeats, replication, commit-index advancement, and term fencing. Pure state machine,
  independent of HTTP.
- `queue-service-api`: the queue node's HTTP surface — data routes, the `421` wrong-node redirect,
  `/raft/*` cluster routes, the three-token authorisation split, `/health` and `/health/vivo`, the
  declared contract version, and single-node mode.
- `queue-log-application`: how committed log entries apply to `colas.py` state — the operation
  vocabulary, atomicity of compound mutations, master-only lazy expiry replicated as `expirar`
  entries, and post-election catch-up recovery.
- `queue-client-failover`: `ClienteReplica` — seed list, master discovery and caching, one-shot
  redirect following without POST duplication, backoff during elections, and the connection-failure
  versus wrong-node distinction.
- `balanceador-queue-integration`: how the balanceador consumes the above — seed-list configuration,
  `derivar()` / `recolectar()` status handling, and the additive `/health` cluster fields.

### Modified Capabilities

None.

## Approach

Approach 1 from the exploration: follow the plan — hand-rolled Raft-lite, standard library only.
Not re-opened.

Three things shape *how*, beyond the plan:

**The state machine is extracted from the transport.** `raft.py` holds terms, votes, the log, the
"at least as up to date" comparison, and commit-index advancement as plain objects driven by an
**injectable clock**, with no sockets and no real timers. This is the single highest-value seam:
election safety and log-matching bugs hide there, and election timeout plus jitter is otherwise
inherently flaky to test. The injectable clock — or an explicit "force election now" hook — is not
optional. HTTP transport sits on top and is exercised separately by in-process
`ThreadingHTTPServer` instances on `127.0.0.1`, the pattern `tests/test_servidor_cola.py` already
uses; "kill a node" is `shutdown()`, "partition" is a transport that drops connections between
specific peers.

**Uncommitted expiry blocks `tomar()`.** `_proximo_vivo()` (`colas.py:267-276`) is a second,
synchronous expiry path evaluated inside every `tomar()` against the local clock. Once expiry is a
log-committed decision, `tomar()` can no longer settle it alone. Of the two options the plan leaves
open, we take **(b): block `tomar()` until the `expirar` entry commits**. It costs latency only on
the calls that actually meet a dead head-of-queue item, whereas option (a) lets the master hand out
an obviously dead pedido while its expiry record is still in flight — trading a correctness property
for latency on a path that is already a long-poll. `sdd-design` owns the exact mechanism.

**Single-node mode is a first-class path, not an assumption.** The worker contract already promises
a one-element seed list works. A lone node declares itself master trivially (majority of 1), never
emits `421`, and behaves byte-for-byte as today — which is also what keeps the existing HTTP tests
meaningful and lets the worker team keep developing against the current single node.

### Delivery — chained slices

Delivery strategy is `auto-chain`, and this work will comfortably exceed the 400-line review budget,
so it is planned as independently shippable slices from the start. Each has its own verification and
its own rollback.

| # | Slice | Repo | Why it stands alone |
|---|-------|------|---------------------|
| 1 | `421` contract update in both documents | balanceador | Docs only, no code. Unblocks the worker team immediately. |
| 2 | Test harness + `raft.py` state machine, RED-first | colas_serv | Pure logic, no HTTP, no wiring. Nothing consumes it yet. |
| 3 | Role/term state, `/raft/*`, split tokens, `/health` fields, `421`, single-node mode | colas_serv | Cluster protocol live; data path still applies directly. |
| 4 | Log-commit application, master-gated recoverer, post-election catch-up | colas_serv | Flips mutations onto the log. The riskiest slice; isolated deliberately. |
| 5 | `ClienteReplica` + `tests/test_cluster_cola.py` | balanceador | New module against a fake cluster; nothing imports it yet. |
| 6 | `balanceador.py` / `consola.py` wiring, seed list, `salud()` fields; delete `cola/` | balanceador | The cutover. Reversible by env var until `cola/` is removed. |

Slices 2–4 and 5–6 are each internally ordered; slice 1 blocks nothing and should go first.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `docs/contrato-worker.md` | Modified | `421` replaces `409 no-soy-master`. **External deliverable** already handed to the worker team — ship first and announce. |
| `docs/plan-cola-desacoplada.md` | Modified | Same redirect change across Regla 1, discovery, and the contract section. |
| `sdypp_colas_serv/raft.py` | New | Election, heartbeats, replication, term fencing, commit index. Injectable clock. |
| `sdypp_colas_serv/servidor.py` | Modified | Role/term, `/raft/*`, `421`, three tokens, `/health` + `/health/vivo`, master-gated recoverer. |
| `sdypp_colas_serv/colas.py` | Modified | Business logic unchanged; invoked only from the committed-entry apply step. `_proximo_vivo()` gains the uncommitted-expiry rule. |
| `sdypp_colas_serv/tests/` | New | `test_raft.py`, plus `test_colas.py` and `test_servidor_cola.py` relocated from the balanceador. |
| `sdypp_colas_serv/README.md` | Modified | Raft-lite protocol, the four correctness rules, and "no proxy in front of the cluster". |
| `app/clientereplica.py` | New | `ClienteReplica` — seed list, cached master, redirect following, backoff. |
| `app/clientecola.py` | Modified/Removed | Superseded by `ClienteReplica`; `ErrorCola` and the stale-connection retry (`:87-132`) are preserved, since that retry is what keeps a redirect layer from re-sending a POST of unknown delivery status. |
| `app/balanceador.py` | Modified | `BA_COLA_URL` comma split (`:73`, `:288`), `derivar()` (`:386-441`), `recolectar()` (`:346-376`), `salud()` (`:550-604`). |
| `consola.py` | Modified | Generate a seed list instead of a single URL (`:100`, `:641`). |
| `tests/test_cluster_cola.py` | New | Fake cluster: cold-start discovery, redirect without POST duplication, backoff while leaderless, never writes to a slave. |
| `cola/` | Removed | Verified duplicate of `sdypp_colas_serv`; removed in the final slice. |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Raft-lite correctness bugs (split-brain, lost entries, bad commit advancement) | High | Extract the state machine as pure logic with an injectable clock; RED-first tests for majority election, stale-log rejection, term fencing, and "never two masters" — including under simulated partition. |
| Election-timing tests become flaky in CI | High | No wall-clock races: injectable clock or explicit "force election" hook, seeded/disabled jitter, short deterministic timeouts. Called out in the exploration as non-optional. |
| The worker team codes against `409` before the contract update lands | Med | Slice 1 ships first and is announced. The contract already told them to branch on the body, not the status, which makes their change small. |
| Post-election catch-up missed → fresh master serves a dead pedido | Med | Explicitly in scope (item 10) with its own spec requirement and test, rather than left as an implicit assumption. |
| Blocking `tomar()` on uncommitted expiry adds tail latency | Med | Only on calls meeting an expired head-of-queue item, on a path that is already a long-poll. Accepted deliberately over serving dead work. |
| `ClienteReplica` duplicates a POST while following a redirect | Med | Preserve `ErrorCola.enviado` and the existing rule that only a stale reused connection is retried, never a fresh one. A `421` is a definitive "not processed" and is safe to re-send; a connection failure is not. |
| Write latency grows (majority ack per publish) | Med | Known and accepted trade-off, already documented in the plan. Report it in the informe; it is the price of the no-loss guarantee. |
| Two-repo lockstep breaks (`421` in one repo, `409` in the other) | Med | Slice 1 changes only documents; the server emits `421` in slice 3 and the client understands it in slice 5, so the server is ahead of the client — never the reverse. |
| Cutover regresses the single-node path | Low | Single-node mode is an explicit requirement with the existing `test_servidor_cola.py` as its regression guard. |

The contract version stays **`1.0`**: although "changing what a code means" is a v2 event by the
plan's own rules, no running node has ever emitted `409 no-soy-master` — `docs/contrato-worker.md`
states the current node "nunca lo devuelve". Only the document changes, so no deployed behaviour is
being redefined. `sdd-spec` should confirm this reading before freezing the wire contract.

## Rollback Plan

Per slice, in reverse order:

- **Slice 6** (cutover): the highest-risk step and the only one users notice. `cola/` removal is the
  point of no easy return, so keep it last within the slice. Before that, rollback is
  `git revert` of the balanceador commits and pointing `BA_COLA_URL` back at a single URL; the old
  `ClienteCola` path stays importable until the slice lands.
- **Slice 5**: `ClienteReplica` is a new unreferenced module — deleting it changes nothing.
- **Slice 4** (log application): `git revert` restores direct mutation. The cluster keeps electing
  and heartbeating (slice 3 stands alone) but stops replicating data — degraded to today's
  single-node durability, not broken. Committed in-memory state is lost on revert; drain the cluster
  first.
- **Slice 3**: revert returns `servidor.py` to a plain single node. The `/health` additions are
  additive, so any consumer reading them degrades to "field absent", not to an error.
- **Slice 2**: `raft.py` is unreferenced — deleting it changes nothing.
- **Slice 1**: `git revert` the documents and tell the worker team. Since they branch on the body,
  supporting both `409` and `421` in their client is a two-line tolerance either direction.

Operationally, at any point: run one queue node with a one-element `BA_COLA_URL`. Single-node mode
is behaviourally identical to today, so "shrink the cluster to one" is always a valid panic button.

## Dependencies

- Python 3.13 standard library only in `sdypp_colas_serv` — the Docker image does not run
  `pip install`. This is a hard constraint on every design choice in that repo.
- `sdypp_colas_serv` needs a stdlib `unittest` test command established before any Strict TDD work
  lands there (`openspec/config.yaml` records `test_command: null` today).
- Balanceador baseline: `./.venv/bin/python -m unittest discover -s tests` — 100 tests passing.
  No slice may reduce that number without an explicit, stated reason.
- The worker team must be told when slice 1 merges. Not a blocker for us; a blocker for them.
- Both repos are on branch `feature/desacople`; the subtree split is already verified byte-identical.

## Success Criteria

- [ ] Both `docs/contrato-worker.md` and `docs/plan-cola-desacoplada.md` describe `421 Misdirected
      Request` with no remaining `409 no-soy-master` reference and no "decisión pendiente" note.
- [ ] `sdypp_colas_serv` has a runnable stdlib `unittest` command, and `openspec/config.yaml` records
      it.
- [ ] `test_raft.py` covers, RED-first: majority election at 3 and 5 nodes; a candidate with a stale
      log cannot win; a node seeing a higher term steps down; and no two masters coexist, including
      under simulated partition.
- [ ] A 3-node local cluster shows exactly one `"rol": "master"` in `/health`; the other two report
      `"slave"` with a matching `masterConocido`.
- [ ] Killing the master with `202`-confirmed requests in flight loses none of them: the newly
      elected master delivers them.
- [ ] A newly elected master runs catch-up recovery before its first `tomar()`, and never hands out
      a pedido whose reservation expired during the leaderless window.
- [ ] A revived zombie master commits nothing, steps down on first contact, and starts answering
      `421`.
- [ ] A non-master node answers `421` on every data route; `/respuestas` still answers `409` for
      `desconocido` and `destinatario-saturado`, with no overlap between the two meanings.
- [ ] `ClienteReplica` finds the master cold, follows a `421` without duplicating the POST, backs off
      while the cluster is leaderless, and never sends `/pedidos/tomar` to a node reporting
      `"rol": "slave"`.
- [ ] `recolectar()` distinguishes `204` from `503`/`421`/connection failure and sleeps before
      retrying — with the whole cluster down, balanceador CPU stays near zero.
- [ ] A one-element seed list behaves exactly as the current single node; `test_servidor_cola.py`
      passes unchanged.
- [ ] The balanceador suite still passes at or above its 100-test baseline, and `cola/` is gone with
      its tests relocated.
- [ ] Every slice merged independently, each under the 400-line review budget, each with its own
      verification.
