# Tasks: Decouple the queue into a replicated standalone service (Raft-lite master/slave)

Change: `desacople-cola-replicada`
Repositories: `sdypp_balanceador` (this repo, `feature/desacople`) and
`sdypp_colas_serv` (sibling repo at `/home/juan-cruz/Documents/UNLu/SD/tp-grupal/sdypp_colas_serv`,
`feature/desacople`). Every task below is tagged `(balanceador)` or `(colas_serv)`. Paths tagged
`(colas_serv)` resolve inside the sibling repo root, not this one.

Strict TDD is enabled for both repos. Every behaviour-producing task is split RED (failing test,
observed failing) → GREEN (minimal passing implementation) → REFACTOR (cleanup, still green).
No production code is written before its failing test exists and has been run.

`sdypp_colas_serv` has no `tests/` directory yet — establishing its stdlib `unittest` runner is
task 2.1, a prerequisite for every other `(colas_serv)` RED task.

---

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~4,500–5,000 total (additions + deletions across both repos) |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | 11 chained work units, PR 1 → PR 11 (see below) |
| Delivery strategy | auto-chain |
| Chain strategy | feature-branch-chain |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: feature-branch-chain
400-line budget risk: High

Rationale: `raft.py` + its deterministic `ClusterFalso` test harness alone is realistically
600–900 authored lines; `servidor.py`'s cluster-protocol surface plus its integration tests is
another 700–800; the log-application seam (`aplicar.py`, `colas.py` seams, `motor.py`
propose-and-wait, post-election catch-up) is another 700–800; `ClienteReplica` plus its fake-cluster
tests is ~550–600; and slice 6's `cola/` removal alone deletes on the order of 1,000+ authored
lines (the duplicated `colas.py`, `servidor.py`, `Dockerfile`, `README.md`, plus the two relocated
test files) — deletions count toward the 400-line budget the same as additions. No single slice as
scoped in the proposal fits inside 400 lines; five of the six proposal slices are split below into
two chained PRs each so every individual PR stays reviewable.

The **tracker branch is `feature/desacople`**, already checked out in both repos and already the
integration point for this whole change (it holds the prior planning commits). PR 1 targets
`feature/desacople`; each subsequent PR targets the immediately preceding PR's branch. Only
`feature/desacople` itself is expected to eventually merge to `main`, which stays outside this
change's scope and is a separate future decision.

### Suggested Work Units

| Unit | Goal | Likely PR | Repo | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|------|----------------------|------------------|-------------------|
| 1 | `421` contract update in both docs | PR 1 (base: `feature/desacople`) | balanceador | N/A — docs only; `git diff` review | N/A — no executable change | `git revert` the two doc commits; tell the worker team both codes are tolerated meanwhile |
| 2a | `ClusterFalso` harness + RED `test_raft.py` (no `raft.py` yet) | PR 2a (base: PR 1 branch) | colas_serv | `python3 -m unittest discover -s tests -v` (expected: collection succeeds, tests fail with `ImportError`/`AssertionError`, never `raft.py` present) | N/A — pure unit harness, no sockets | Delete `tests/test_raft.py` + `tests/__init__.py`; nothing else references them |
| 2b | `raft.py` implementation makes `test_raft.py` green (GREEN + REFACTOR) | PR 2b (base: PR 2a branch) | colas_serv | `python3 -m unittest discover -s tests -v` (all `test_raft.py` cases pass) | N/A — pure unit test, no sockets | Delete `raft.py`; `test_raft.py` reverts to RED, no other file imports it yet |
| 3a | `servidor.py` cluster protocol: role/term, `/raft/*`, three tokens, `/health`+`/health/vivo`, `421`, single-node mode | PR 3a (base: PR 2b branch) | colas_serv | `python3 -m unittest discover -s tests -v` (existing `test_servidor_cola.py` unchanged and green; new token/421/health tests green) | Single in-process `ThreadingHTTPServer` node on `127.0.0.1`, single-node mode | Revert `servidor.py`; a plain single node remains, `/health` additions are additive so any external reader degrades to "field absent" |
| 3b | `test_cluster_raft.py`: 3-node in-process integration (election, 421, majority ack) | PR 3b (base: PR 3a branch) | colas_serv | `python3 -m unittest discover -s tests -v` | 3× `ThreadingHTTPServer` on `127.0.0.1` free ports, `shutdown()` for "kill", `threading.Event` for "slow slave" | Delete `tests/test_cluster_raft.py`; nothing else depends on it |
| 4a | `aplicar.py` + `colas.py` seams (clock injection, `inspeccionar_frente`/`esperar_cambio`, expiry decide/apply split, `responder()` saturation pre-check) | PR 4a (base: PR 3b branch) | colas_serv | `python3 -m unittest discover -s tests -v` (relocated `test_colas.py` green; new `test_aplicar.py` green) | N/A — pure unit tests with `RelojFalso` | Revert `aplicar.py` + the `colas.py` seam commit; direct-mutation path (pre-existing) is restored in the same revert |
| 4b | `motor.py` propose-and-wait, applier thread, `ids_propuestos()`, master-gated `recuperador()`, post-election catch-up | PR 4b (base: PR 4a branch) | colas_serv | `python3 -m unittest discover -s tests -v` (post-election catch-up integration test green; full suite green) | 3-node `ThreadingHTTPServer` cluster, `shutdown()` to force failover, `threading.Event` to gate a delayed `appendEntries` reply | Revert `motor.py`'s propose-and-wait/applier/catch-up commit; cluster keeps electing and heartbeating (3a/3b stand alone) but stops replicating data — degraded to single-node durability. Drain the cluster first: committed in-memory state is lost on revert |
| 5a | `app/clientereplica.py` (`ClienteReplica`): seed list, discovery, master cache, one-shot redirect, backoff, allowlisted redirect target | PR 5a (base: PR 4b branch) | balanceador | `./.venv/bin/python -m unittest discover -s tests` | N/A — new unreferenced module, unit-level with scripted `http.server` stand-ins | Delete `app/clientereplica.py`; nothing imports it yet |
| 5b | `tests/test_cluster_cola.py`: fake-cluster integration (cold-start, redirect, backoff, never-write-to-slave, redirect-loop bound) | PR 5b (base: PR 5a branch) | balanceador | `./.venv/bin/python -m unittest discover -s tests` | Minimal `http.server` fake nodes returning scripted `421`/`503`/health bodies | Delete `tests/test_cluster_cola.py`; `ClienteReplica` stays unreferenced |
| 6a | `balanceador.py`/`consola.py` cutover: seed list, `derivar()`/`recolectar()` status handling, `salud()` fields | PR 6a (base: PR 5b branch) | balanceador | `./.venv/bin/python -m unittest discover -s tests` (≥100 tests, at or above baseline) | Real 1-node then 3-node local `sdypp_colas_serv` cluster for the manual chaos checklist | `git revert` the balanceador commits, point `BA_COLA_URL` back at a single URL; old `ClienteCola` path stays importable |
| 6b | Delete `cola/`, relocate `tests/test_colas.py`/`tests/test_servidor_cola.py` off this repo | PR 6b (base: PR 6a branch) — **candidate for `size:exception`**, maintainer-approved: pure verified-duplicate deletion, not new logic | balanceador | `./.venv/bin/python -m unittest discover -s tests` (suite green with `cola/` gone; no import of `cola.*` remains) | N/A — deletion only | Point of no easy return; keep last. Restore from `git revert` if the balanceador suite regresses |

---

## Phase 1: Slice 1 — Wire contract documentation (`421`) (balanceador)

External deliverable, blocks nothing else, ships and is announced first.

- [x] 1.1 Update `docs/contrato-worker.md`: collapse the "⚠ Colisión de códigos" box to a plain
      two-row table, remove the "decisión pendiente" note, and change the pseudocode branch
      `codigo == 409 and respuesta.get("error") == "no-soy-master"` to `codigo == 421`. No other
      wording changes. — ~15 lines changed.
- [x] 1.2 Update `docs/plan-cola-desacoplada.md`: apply the same `409 no-soy-master` → `421` change
      across Regla 1, the master-discovery table, the `CONTRATO.md` section, and every route
      listing that mentions the old code. — ~35 lines changed.
- [x] 1.3 Verify: `git diff` shows no remaining literal `409` next to `no-soy-master` in either
      file, and no "decisión pendiente" string remains. Both files still reference the two
      unaffected `409` meanings on `/respuestas` (`desconocido`, `destinatario-saturado`) unchanged.
      (Spec: `queue-service-api` — Contract Version Declaration Stays 1.0, scenario "The two
      /respuestas 409 meanings are unaffected".)
- [x] 1.4 Announce to the worker team that slice 1 has merged and `421` replaces `409
      no-soy-master`, per the proposal's Dependencies section (not a code task, but a recorded
      delivery step). Recorded here as the delivery step; actual team notification is an
      out-of-band action for the user to perform once this slice is merged/pushed.

---

## Phase 2: Slice 2 — Raft-lite state machine, RED-first (colas_serv)

### 2a — Test harness + RED `test_raft.py`

- [ ] 2.1 (RED, prerequisite) Create `sdypp_colas_serv/tests/__init__.py` (empty) and record the
      test command `python3 -m unittest discover -s tests -v` in `sdypp_colas_serv`'s SDD config
      (mirrors `openspec/config.yaml`'s per-repo `test_command` field for `sdypp_colas_serv`,
      currently `null`). Verify: `python3 -m unittest discover -s tests` runs and reports "no
      tests found" cleanly (proves the runner itself works before any test exists). — ~15 lines.
- [ ] 2.2 (RED) Create `sdypp_colas_serv/tests/test_raft.py` with the `ClusterFalso` harness:
      `__init__(n, semilla)`, `avanzar(ms, nodos=None)`, `entregar(veces=None)`,
      `particionar(grupo_a, grupo_b)`, `sanar()`, `forzar_eleccion(nodo)`, `estabilizar(ms=2000)`,
      `masters()`. The harness MUST import no `raft` module yet — it is written against the
      not-yet-existing `NodoRaft` interface from Design Decision 4. **This file MUST NOT import
      `time` and MUST NOT call `sleep` anywhere** — all timing is `avanzar(ms)` against a fake
      clock argument. — ~220 lines.
- [ ] 2.3 (RED) Write the majority-election test cases against `ClusterFalso`/`NodoRaft`:
      3-node and 5-node majority election (spec `queue-replication` scenarios "Majority election
      succeeds at 3 nodes" / "at 5 nodes"). Run: fails with `ImportError` (`raft.py` does not
      exist). — ~60 lines.
- [ ] 2.4 (RED) Write the stale-log-candidate test cases: older last-term loses (scenario
      "Candidate with a stale log cannot win"), and equally-fresh-but-shorter-log tiebreak loses
      (scenario "Candidate with an equally fresh but shorter log loses the tiebreak"). — ~40 lines.
- [ ] 2.5 (RED) Write the single-vote-per-term test (scenario "A node votes at most once per
      term") and the term-only-increases test (scenario "Term only increases, never decreases").
      — ~30 lines.
- [ ] 2.6 (RED) Write the term-fencing/no-split-brain test cases: master steps down on higher term
      (scenario "A master sees a higher term and steps down"), no two masters under a healed
      partition, no two masters during an active partition — using `particionar()`/`sanar()`, and
      the **property-style 200-step seeded-schedule safety loop** asserting `len(masters()) <= 1`
      after every `entregar()`, including the explicit zombie-master case (old master revived,
      sees higher term on first contact, steps down, commits nothing). — ~90 lines.
- [ ] 2.7 (RED) Write heartbeat/election-timeout test cases: a slave that stops receiving
      heartbeats starts an election (scenario "A slave that stops receiving heartbeats starts an
      election"), a slave receiving heartbeats never starts one (scenario "A slave receiving
      heartbeats never starts an election"), using `avanzar()` only — no real timers. — ~40 lines.
- [ ] 2.8 (RED) Write commit-index-advancement test cases: an entry commits only after majority
      ack (scenario "An entry commits only after majority acknowledgement"); commit index never
      advances on a prior-term entry alone but does advance transitively once a same-term entry
      commits (Interfaces section, `avanzar_commit`'s "NOT `break`" rule); a slower slave that fell
      behind receives missed entries in order before it can ack the latest index (scenario "A
      slower slave still receives entries it missed, in order"); the conflict-term hint resync
      (Decision 11 — `terminoConflicto`/`primerIndiceDelTermino`, master jumps `siguienteIndice`
      instead of decrementing by one). — ~60 lines.
- [ ] 2.9 (RED) Write the injectable-clock/force-election test (scenario "A test forces an
      election without waiting on wall-clock time") and the single-node `mayoria == 1` test
      (Interfaces section). — ~25 lines.
- [ ] 2.10 Verify RED: run `python3 -m unittest discover -s tests -v` and confirm every test in
      `test_raft.py` fails (collection error or assertion failure), none pass, and no test imports
      `time` or calls `sleep` (`grep -n "^import time\|time\.sleep" tests/test_raft.py` returns
      nothing). This is the explicit RED checkpoint before any `raft.py` code exists.

### 2b — `raft.py` implementation (GREEN + REFACTOR)

- [ ] 2.11 (GREEN) Create `sdypp_colas_serv/raft.py`: `NodoRaft.__init__(yo, pares, reloj, azar,
      timeout_eleccion_ms, heartbeat_ms)` with no imports beyond `dataclasses`/`typing` — no
      `threading`, `time`, `http`, or module-level `random` use. Implement role/term state,
      `voto_para`, the append-only log (Decision 4, spec `queue-replication` — Node Roles and
      Persistent Term State). — ~50 lines.
- [ ] 2.12 (GREEN) Implement `tic(ahora_ms) -> list[Mensaje]`: election-timeout detection with
      injected `azar` jitter, heartbeat emission on the master path. Run tests 2.7 and 2.9 to
      green. — ~60 lines.
- [ ] 2.13 (GREEN) Implement `recibir_solicitud_voto`/`recibir_respuesta_voto`: majority election,
      the "at least as up to date" vote rule (last term first, index on tie), one-vote-per-term.
      Run tests 2.3, 2.4, 2.5 to green. — ~70 lines.
- [ ] 2.14 (GREEN) Implement `recibir_append`/`recibir_respuesta_append`: log matching, the
      conflict-term hint (`terminoConflicto`/`primerIndiceDelTermino`), commit-index advancement
      per `avanzar_commit` (same-term-only direct count, prior-term rides along). Run test 2.8 to
      green. — ~80 lines.
- [ ] 2.15 (GREEN) Implement `_fencing(termino_ajeno)` called as the first statement of all four
      `recibir_*` methods (Interfaces section). Run test 2.6 to green, including the 200-step
      seeded safety loop. — ~25 lines.
- [ ] 2.16 (GREEN) Implement `proponer(operacion, payload) -> (indice, termino) | None`,
      `entradas_a_aplicar() -> list[Entrada]` (advances `indice_aplicado`, does not push via
      callback), and `instantanea() -> dict`. `mayoria = (len(pares) + 1) // 2 + 1`. Run remaining
      tests to green. — ~35 lines.
- [ ] 2.17 (REFACTOR) Clean up `raft.py`: remove duplication between the four `recibir_*` entry
      points, confirm no stray `time`/`threading`/`http` import, re-run full `test_raft.py` green.
      — ~0 net lines (refactor only).
- [ ] 2.18 Verify: `python3 -m unittest discover -s tests -v` — all of `test_raft.py` green;
      confirm via `grep` that `raft.py` imports only `dataclasses`/`typing`.

---

## Phase 3: Slice 3 — Cluster protocol wiring (colas_serv)

### 3a — `servidor.py` role/term, `/raft/*`, tokens, `/health`, single-node mode

- [ ] 3.1 (RED — threat matrix: Cluster protocol authorisation) Write
      `tests/test_servidor_cola.py` additions asserting `POST /raft/appendEntries` and
      `POST /raft/requestVote` reply `403 {"error": "token inválido"}` when called with the
      publisher token, the consumer token, or no token, and that `/raft/estado`'s term is
      unchanged after each rejected call. (Spec `queue-service-api` — Cluster-Internal Routes,
      scenario "An outside caller cannot join the cluster protocol".) — ~30 lines.
- [ ] 3.2 (RED — threat matrix: Token split) Write one `403` test per route × wrong-token-class:
      `/pedidos` with consumer token, `/pedidos/tomar`|`/pedidos/devolver`|`/respuestas` with
      publisher token, `/respuestas/tomar` with consumer token. (Spec `queue-service-api` —
      Three-Way Token Authorization Split, scenario "A consumer token cannot publish".) —
      ~35 lines.
- [ ] 3.3 (RED) Write `421` tests: a slave replies `421 {"error":"no-soy-master","master":"<url>"}`
      without mutating state; a node with no known master replies `421` with `"master": null`
      during an election; `421` never appears on `/health`, `/raft/estado`, `/raft/appendEntries`,
      or `/raft/requestVote`. (Spec `queue-service-api` — Wrong-Node Redirect on Every Data Route,
      all three scenarios.) — ~40 lines.
- [ ] 3.4 (RED) Write `/health` and `/health/vivo` tests: `/health` includes `rol`, `termino`,
      `masterConocido`, `contrato: "1.0"`, `instancia`, existing depth fields; `masterConocido` is
      `null` mid-election; `/health/vivo` requires no token and replies `200 {"vivo": true}` even
      while `candidato` with no known master, exact body shape only (threat matrix: Unauthenticated
      liveness). (Spec `queue-service-api` — /health Reports Cluster and Contract State,
      /health/vivo Liveness Endpoint.) — ~35 lines.
- [ ] 3.5 (RED) Write single-node-mode tests: a node with a one-element seed list never emits
      `421` on any data route and reports `rol: "master"` immediately. Confirm the **existing**
      `test_servidor_cola.py` cases are re-run unmodified against this mode and MUST still pass
      once 3.6–3.9 land. (Spec `queue-service-api` — Single-Node Mode, both scenarios.) —
      ~20 lines.
- [ ] 3.6 Verify RED: run `python3 -m unittest discover -s tests -v`; confirm 3.1–3.5 fail for the
      expected reason (missing routes/fields), and the pre-existing `test_servidor_cola.py` cases
      still pass unchanged (nothing here has touched the data-route handlers yet).
- [ ] 3.7 (GREEN) `servidor.py`: add role/term state (`rol`, `termino_actual`, `instancia`), read
      `COLA_PARES` (comma-separated peer URLs; empty ⇒ single-node mode per Decision 9 — no
      `if len(pares) == 1` branch in the data path, `mayoria` computed uniformly). Wire a
      `NodoRaft` instance per node. — ~50 lines.
- [ ] 3.8 (GREEN) `servidor.py`: split `COLA_TOKEN` into `COLA_TOKEN_PUBLICADOR` /
      `COLA_TOKEN_CONSUMIDOR` / `COLA_TOKEN_CLUSTER`, each falling back to `COLA_TOKEN` when unset
      (Migration section). Add the per-route token-class check before any handler body runs. Run
      test 3.2 to green. — ~45 lines.
- [ ] 3.9 (GREEN) `servidor.py`: add `POST /raft/appendEntries`, `POST /raft/requestVote`,
      `GET /raft/estado`, gated by `COLA_TOKEN_CLUSTER`. Start `sdypp_colas_serv/motor.py`'s
      networking half: the `tic` thread driving `NodoRaft.tic()` on `RAFT_HEARTBEAT_MS`/
      `RAFT_ELECCION_TIMEOUT_MS`/`RAFT_JITTER_MS` (env, with the documented defaults), and the
      peer HTTP client that delivers `NodoRaft`'s returned `Mensaje`s over `/raft/appendEntries`
      and `/raft/requestVote` (reusing the pooled `http.client` pattern from `clientecola.py`).
      Run test 3.1 to green. — ~140 lines (`motor.py` networking half).
- [ ] 3.10 (GREEN) `servidor.py`: add the wrong-node-redirect gate on the five data routes,
      answering `421` with the current `masterConocido` (or `null`) before any mutation or commit
      counting. Run test 3.3 to green. — ~35 lines.
- [ ] 3.11 (GREEN) `servidor.py`: extend `/health` with `rol`/`termino`/`masterConocido`/
      `instancia`/`contrato: "1.0"`; add `/health/vivo` as a distinct unauthenticated route
      returning only `{"vivo": true}`. Run test 3.4 to green. — ~30 lines.
- [ ] 3.12 (GREEN) Confirm single-node mode requires no special-casing beyond `mayoria = 1` and
      an empty `COLA_PARES`; run test 3.5 and the full pre-existing `test_servidor_cola.py` suite
      to green, unchanged. — ~10 lines (env wiring only, if anything).
- [ ] 3.13 (REFACTOR) Clean up the route-dispatch table in `servidor.py` now carrying three token
      classes plus `/raft/*`; re-run the full colas_serv suite green.
- [ ] 3.14 Verify: `python3 -m unittest discover -s tests -v` green; `docs/` note not required yet
      (README update is task 4.10-adjacent, tracked in 3b below for the protocol section).

### 3b — 3-node integration tests

- [ ] 3.15 (RED) Create `sdypp_colas_serv/tests/test_cluster_raft.py`: spin up 3 in-process
      `ThreadingHTTPServer` nodes on `127.0.0.1` free ports (the pattern
      `test_servidor_cola.py` already uses). Assert exactly one `rol: "master"` across `/health`
      after `estabilizar()`-equivalent polling (bounded, no fixed `sleep`). — ~60 lines.
- [ ] 3.16 (RED) Assert a data route on a slave returns `421` with the correct `master` URL; a
      `/respuestas` conflict still returns `409 {"resultado":"desconocido"}` with no overlap with
      `421`. (Threat matrix already covered structurally in 3.3/3.4; this is the multi-process
      confirmation.) — ~30 lines.
- [ ] 3.17 (RED) Assert `202` for `POST /pedidos` does not return before a majority acks: delay one
      slave's `/raft/appendEntries` handler using a **`threading.Event` the test controls** (never
      `sleep`), post a pedido, confirm the response is not sent until the event is set and at
      least 1 of 2 slaves has acknowledged. (Spec `queue-service-api` — POST /pedidos, scenario "A
      pedido is only acknowledged after majority commit".) — ~40 lines.
- [ ] 3.18 (RED) Kill the master (`shutdown()`) with pedidos in flight and assert a new master is
      elected and reports `rol: "master"` within a bounded polling window (no fixed `sleep`;
      poll `/health` with a ceiling). — ~35 lines.
- [ ] 3.19 (RED) Assert a revived slave catches up within a small bounded number of heartbeats
      using the conflict-term hint from Decision 11, not one-at-a-time backtracking. — ~25 lines.
- [ ] 3.20 Verify RED: run the new `test_cluster_raft.py`; expect failures only where slice 4's
      log-application behaviour is required (majority-ack-gated `202` needs `motor.py`'s
      propose-and-wait, landed in 4b) — document which cases are expected to stay red until 4b and
      which are already green from 3a.
- [ ] 3.21 (GREEN) Fix any wiring gap surfaced by 3.15/3.16/3.18/3.19 that belongs to slice 3 (not
      to log application) — e.g. `/health`'s `masterConocido` propagation, election timing
      defaults. Re-run to green everything not explicitly deferred to 4b. — ~20 lines.
- [ ] 3.22 (REFACTOR) Extract any repeated "spin up N nodes on free ports" boilerplate shared with
      `test_servidor_cola.py` into a small test helper, keep both files green.
- [ ] 3.23 Update `sdypp_colas_serv/README.md`: document the Raft-lite protocol overview, the
      three-token split, `/raft/*`, and "no proxy in front of the cluster" (the load-balancer must
      talk to every node directly, per Decision 10's discovery model). — ~40 lines.
- [ ] 3.24 Verify: full `sdypp_colas_serv` suite green (`test_raft.py`, `test_aplicar.py`-not-yet-
      existing is fine, `test_servidor_cola.py`, `test_cluster_raft.py` with 3.17's majority-ack
      case explicitly marked as still-red-until-4b or already resolved per 3.21's outcome).

---

## Phase 4: Slice 4 — Log-commit application, master-gated recovery (colas_serv)

This is the highest-risk slice (per the proposal's risk table) — isolated deliberately.

### 4a — `aplicar.py` + `colas.py` seams

- [ ] 4.1 (RED) Extend `sdypp_colas_serv/tests/test_colas.py` (already relocated per 2.1's test
      directory, copied from `sdypp_balanceador/tests/test_colas.py` — see Phase 2 note below)
      with: `Pedido`/`ColaPedidos`/`ColaRespuestas`/`Sistema` accept an injected `reloj` callable
      (default `time.monotonic`); `Pedido.encolado_en` is a required field, not a
      `default_factory`. (Design Decision 2, Seam A.) — ~25 lines.
- [ ] 4.2 (RED) Write tests for `ColaPedidos.inspeccionar_frente(ahora_ms, excluidos)`: pure,
      mutates nothing, returns `("vacio", None)` / `("vencidos", [ids])` / `("vivo", pedido_id)`;
      idempotent under repeated calls. (Spec `queue-log-application` — Uncommitted Expiry Blocks
      tomar(), and Design Decision 5.) — ~30 lines.
- [ ] 4.3 (RED) Write tests for `ColaPedidos.detectar_vencidos(ahora_ms)` /
      `ColaRespuestas.detectar_purgables(corte)` (pure decision) and
      `ColaPedidos.aplicar_expiry(decision)` / `ColaRespuestas.aplicar_purga(purga)` (pure
      application): applying the same `DecisionExpiry` twice is a no-op the second time. (Design
      Decision 2, Seam B.) — ~40 lines.
- [ ] 4.4 (RED) Write tests for `Sistema.decidir_expiry(ahora_ms)` / `Sistema.expirar(decision)`
      replacing `Sistema.recuperar()`; `reservar_pedido`, `devolver_pedido`, `retirar_respuesta`,
      `pedido_en_vuelo` added to the `Sistema` surface (Decision 1's operation-to-method table).
      — ~35 lines.
- [ ] 4.5 (RED — atomicity) Write the `responder()` saturation pre-check test:
      `Sistema.responder()` evaluates saturation **before** `pedidos.completar()`, under both
      locks, and returns `(False, "destinatario-saturado")` without mutating anything (no partial
      apply). (Spec `queue-log-application` — Compound Mutations Apply Atomically, scenario
      "responder's two mutations are indivisible".) — ~25 lines.
- [ ] 4.6 (RED) Create `sdypp_colas_serv/tests/test_aplicar.py`: applying an identical entry
      sequence to two fresh `Sistema` instances yields byte-identical `estado()`/`como_json()`
      output; every operation is a tolerant no-op against a stale target (id already gone) — never
      raises; an unknown `operacion` is skipped, not raised. (Spec `queue-log-application` —
      Mutations Apply Only From Committed Log Entries, and Design Decision 6.) — ~90 lines.
- [ ] 4.7 Verify RED: run `python3 -m unittest discover -s tests -v`; confirm 4.1–4.6 fail for the
      expected reason (`aplicar.py` does not exist yet; `colas.py` lacks the new methods).
- [ ] 4.8 (GREEN) `colas.py`: inject `reloj` into `Pedido`/`ColaPedidos`/`ColaRespuestas`/`Sistema`;
      make `Pedido.encolado_en` a required field; convert `vence_en`/`reservado_hasta` to absolute
      epoch milliseconds (`venceEnMs`/`reservadoHastaMs`) stamped by the caller (Design Decision 3);
      `queda()` becomes `(self.vence_en_ms - self.reloj_ms()) / 1000`. Run test 4.1 to green. —
      ~40 lines.
- [ ] 4.9 (GREEN) `colas.py`: implement `inspeccionar_frente`, remove the destructive
      `_proximo_vivo()` and `_a_fallar`. Run test 4.2 to green. — ~35 lines.
- [ ] 4.10 (GREEN) `colas.py`: implement `detectar_vencidos`/`detectar_purgables` (pure decision)
      and `aplicar_expiry`/`aplicar_purga` (pure application, idempotent replay). Run test 4.3 to
      green. — ~55 lines.
- [ ] 4.11 (GREEN) `colas.py`: implement `Sistema.decidir_expiry`, `Sistema.expirar`,
      `reservar_pedido`, `devolver_pedido`, `retirar_respuesta`, `pedido_en_vuelo`; apply the
      `responder()` saturation pre-check. Run tests 4.4 and 4.5 to green. — ~60 lines.
- [ ] 4.12 (GREEN) Create `sdypp_colas_serv/aplicar.py`: `Aplicador(sistema).aplicar(entrada) ->
      resultado`, the six-operation dispatch table (`encolar`→`publicar_pedido`,
      `tomar`→`reservar_pedido`, `devolver`→`devolver_pedido`, `responder`→`responder`,
      `retirar-respuesta`→`retirar_respuesta`, `expirar`→`expirar`) plus `sentinela` (no-op).
      Unknown `operacion` logged and skipped, never raised. Run test 4.6 to green. — ~70 lines.
- [ ] 4.13 (REFACTOR) Confirm `test_colas.py` (relocated) is entirely green with only additive
      changes to the pre-existing FIFO/retry/late-answer test cases (design's stated invariant:
      "Everything else in `colas.py`... is untouched"). Re-run full `sdypp_colas_serv` suite green.
- [ ] 4.14 Verify: `python3 -m unittest discover -s tests -v` green for `test_colas.py` and
      `test_aplicar.py`.

### 4b — `motor.py` propose-and-wait, applier, post-election catch-up

- [ ] 4.15 (RED) Extend `sdypp_colas_serv/tests/test_cluster_raft.py` (or a new
      `test_motor.py`, colocated) to unblock 3.17: `POST /pedidos` does not return `202` before a
      majority holds the entry, using the delayed-`appendEntries` `threading.Event` from 3.17. —
      reuses 3.17's scaffold, ~0 new lines (unskip/complete it here).
- [ ] 4.16 (RED — threat matrix: uncommitted expiry blocking) Write the `tomar()`-blocks-on-expiry
      integration test: the head of the waiting queue is expired-but-uncommitted; `POST
      /pedidos/tomar` does not return that pedido and instead waits (within its long-poll budget)
      until the `expirar` entry commits, then serves the next live pedido. Also test that an
      unrelated live pedido at the head is served without waiting on a different pedido's pending
      `expirar`. (Spec `queue-log-application` — Uncommitted Expiry Blocks tomar(), both
      scenarios.) — ~50 lines.
- [ ] 4.17 (RED) Write the master-gated-recoverer tests: a node stepping down stops its expiry
      sweep thread; a node becoming master starts it only after catch-up completes. (Spec
      `queue-log-application` — Master-Only Lazy Expiry, "recuperador() is gated to the master
      role" / "recuperador() starts when a node becomes master".) — ~35 lines.
- [ ] 4.18 (RED) Write the post-election catch-up tests: a master failed while a reservation was
      about to lapse; the leaderless window (election time) exceeds that deadline; the new master
      runs the `sentinela`-commit barrier then one `expirar` sweep before its first `tomar()`
      resolves, and never hands out the already-dead pedido; a `tomar()` call arriving before
      catch-up completes is held, not served early; catch-up runs exactly once per election win.
      (Spec `queue-log-application` — Post-Election Catch-Up Recovery, all three scenarios.) —
      ~60 lines.
- [ ] 4.19 (RED) Write the `503 {"error":"recuperando"}` test: data routes answer `503` (not
      `421`, since this node is the master) for the duration of catch-up. — ~20 lines.
- [ ] 4.20 Verify RED: run the colas_serv suite; confirm 4.15–4.19 fail for the expected reason
      (`motor.py` lacks propose-and-wait/applier/catch-up).
- [ ] 4.21 (GREEN) `motor.py`: implement `_lock_propuesta`, `proponer_y_esperar(operacion, payload,
      limite)` — appends via `NodoRaft.proponer`, registers a per-index `threading.Event` commit
      waiter, returns `COMPROMETIDO`/`DESTITUIDO`/timeout, per Decision 5's exact mechanism and
      Decision 8's lock-ordering rules (never holds a queue lock while waiting for a commit). Run
      test 4.15 to green. — ~65 lines.
- [ ] 4.22 (GREEN) `motor.py`: implement the dedicated applier thread pulling
      `NodoRaft.entradas_a_aplicar()`, calling `Aplicador.aplicar(entrada)`, setting the matching
      commit-waiter `Event` after applying (never while holding `_lock_propuesta`); `_hay.notify()`
      continues to fire from the apply step only. — ~50 lines.
- [ ] 4.23 (GREEN) `motor.py`: implement `ids_propuestos()` — the master-local set of pedido ids
      named by appended-but-not-yet-applied entries, added at append and removed at apply (Decision
      6). Wire `servidor.py`'s `tomar_pedido` handler to the propose-and-wait loop from Decision 5
      (inspect → propose → loop), using `inspeccionar_frente`/`esperar_cambio`. Run test 4.16 to
      green. — ~55 lines.
- [ ] 4.24 (GREEN) `motor.py`: gate `recuperador()` to the master role, starting only after catch-up
      completes and stopping immediately on step-down. Run test 4.17 to green. — ~30 lines.
- [ ] 4.25 (GREEN) `motor.py`: implement `recuperacion_post_eleccion()` — phase 1 `sentinela`
      commit barrier, phase 2 one eager `decidir_expiry`/`expirar` sweep, then open data routes and
      start `recuperador()`. `servidor.py`: answer `503 {"error":"recuperando"}` on data routes
      while catch-up is in progress. Run tests 4.18 and 4.19 to green. — ~55 lines.
- [ ] 4.26 (REFACTOR) Re-verify Decision 8's lock ordering by inspection: no lock held across I/O
      or across a queue lock; re-run the full colas_serv suite (including 3.15–3.19) green.
- [ ] 4.27 Verify: `python3 -m unittest discover -s tests -v` — full `sdypp_colas_serv` suite
      green, including all of `test_cluster_raft.py`'s majority-ack, kill-master, and catch-up
      cases from 3b.

**Test relocation note (sequencing, per proposal requirement):** `tests/test_colas.py` and
`tests/test_servidor_cola.py` were **copied** (not moved) into `sdypp_colas_serv/tests/` at the
start of task 2.1's window, adapted for the new seams across 4.1–4.13, and kept green throughout
3a/3b/4a/4b. The **original copies in `sdypp_balanceador/tests/`** are left untouched and
continue exercising the frozen, pre-Raft `cola/` duplicate — so both repos have working coverage
at every point until slice 6 deletes the balanceador originals (task 6.9).

---

## Phase 5: Slice 5 — `ClienteReplica` (balanceador)

New module against a fake cluster; nothing imports it yet.

### 5a — `ClienteReplica` implementation

- [x] 5.1 (RED) Create `sdypp_balanceador/app/clientereplica.py` test scaffolding in
      `tests/test_clientereplica.py` (unit-level, scripted stand-in servers): construct
      `ClienteReplica(["http://127.0.0.1:PORT"], token="t")` with a single-element seed list and
      confirm it operates against that one node. (Spec `queue-client-failover` — Constructor
      Accepts a Seed List.) — ~20 lines.
- [x] 5.2 (RED) Write cold-start discovery tests: first probed node reporting `rol: "master"` is
      cached without probing the rest; a slave reporting `masterConocido` is followed; every node
      reporting `masterConocido: null` leaves nothing cached and falls through to backoff. (Spec
      `queue-client-failover` — Cold-Start Master Discovery, all three scenarios.) — ~45 lines.
- [x] 5.3 (RED) Write the steady-state no-extra-probes test: 100 consecutive `tomar_respuesta`
      calls against a cached master issue zero `/health` probes. (Spec `queue-client-failover` —
      Cached Master Used at Zero Discovery Cost, scenario "Steady-state operation issues no extra
      health probes".) — ~20 lines.
- [x] 5.4 (RED) Write the one-shot-redirect test: a stale cached master returns `421` with a
      `master` URL, the client retries the identical operation exactly once at the new location
      and returns that result with no further redirect attempts; a second `421` on the retry falls
      back to full seed-list discovery with backoff, never loops. (Spec `queue-client-failover` —
      One-Shot Redirect Following, all three scenarios.) — ~45 lines.
- [x] 5.5 (RED — threat matrix: Redirect following) Write the redirect-target-allowlist test: a
      `421` with `"master": "http://evil:8085"` (not in the static seed list) is ignored, treated
      as `master: null`, and no request is ever sent to that host — discovery runs instead. —
      ~25 lines.
- [x] 5.6 (RED — threat matrix: Redirect loop) Write the mutual-`421`-pair test: two nodes each
      redirect to the other; the client's request count stays bounded, then it surfaces `503`, no
      unbounded loop. — ~25 lines.
- [x] 5.7 (RED) Write the connection-failure test: a connection reset while contacting the cached
      master invalidates the cache and triggers a fresh discovery probe, distinct from `421`
      handling; a fresh-connection failure with unknown delivery status is never blindly retried —
      it surfaces the same way `ErrorCola` already does, preserving the reused-connection-only
      retry rule. (Spec `queue-client-failover` — Connection Failure Triggers Re-Discovery, both
      scenarios.) — ~35 lines.
- [x] 5.8 (RED) Write the leaderless-backoff test: every seed node reports no known master;
      discovery retries with backoff (sleeps, not busy-loop), giving up with `503` once the
      caller's budget is exhausted. (Spec `queue-client-failover` — Backoff While the Cluster Is
      Leaderless, both scenarios.) — ~30 lines.
- [x] 5.9 (RED) Write the never-writes-to-a-known-slave test: a node previously learned as `rol:
      "slave"` (via a prior `/health` probe or a prior `421`) is never targeted directly for a
      mutating write. (Spec `queue-client-failover` — Never Writes to a Known Slave.) — ~20 lines.
- [x] 5.10 Verify RED: run `./.venv/bin/python -m unittest discover -s tests -v`; confirm 5.1–5.9
      fail with `ModuleNotFoundError` (`app.clientereplica` does not exist yet).
- [x] 5.11 (GREEN) Implement `ClienteReplica.__init__(urls, token="", timeout=5.0, conexiones=8,
      backoff_inicial=0.1, backoff_maximo=1.0, presupuesto=5.0)`: one lazily-created, cached
      `ClienteCola` per seed URL, `_master: str | None` guarded by a `threading.Lock`. Run test 5.1
      to green. — ~35 lines.
- [x] 5.12 (GREEN) Implement cold-start discovery: sequential `GET /health` over the seed list
      starting at a rotating offset, first `rol == "master"` or usable `masterConocido` wins. Run
      test 5.2 to green. — ~40 lines.
- [x] 5.13 (GREEN) Implement the cached-master fast path for `publicar_pedido`/`tomar_respuesta`.
      Run test 5.3 to green. — ~15 lines.
- [x] 5.14 (GREEN) Implement one-shot redirect following with the seed-list allowlist check on the
      `master` field (unknown target → treated as `null`), and the bounded-loop fallback to
      discovery. Run tests 5.4, 5.5, 5.6 to green. — ~50 lines.
- [x] 5.15 (GREEN) Implement connection-failure handling: invalidate cache + re-discover, and
      surface `ErrorCola` for a fresh-connection failure without re-sending. Run test 5.7 to green.
      — ~25 lines.
- [x] 5.16 (GREEN) Implement leaderless backoff (`0.1s` doubling to `1.0s` cap, `±20%` jitter,
      budget-bounded) and the never-write-to-a-known-slave guard. Run tests 5.8, 5.9 to green. —
      ~30 lines.
- [x] 5.17 (GREEN) `app/clientecola.py`: add an explicit `ErrorCola.enviado` attribute; change
      `tomar_respuesta()` to stop collapsing every non-200 into `None` — return `(codigo, datos)`
      and let `ClienteReplica` decide. Confirm `clientecola.py`'s own tests still pass unchanged.
      — ~25 lines.
- [x] 5.18 (REFACTOR) Confirm `ClienteReplica` never rewrites `_pedir`'s retry rule (Decision 10) —
      it is a routing layer strictly above the unchanged transport. Re-run full test file green.
- [x] 5.19 Verify: `./.venv/bin/python -m unittest discover -s tests -v` — all of
      `tests/test_clientereplica.py` green; suite count still at or above 100 (this module is
      additive and unreferenced elsewhere so far).

### 5b — Fake-cluster integration tests

- [x] 5.20 (RED) Create `sdypp_balanceador/tests/test_cluster_cola.py`: minimal scripted
      `http.server` fake nodes. Test cold-start discovery against a fake 3-node cluster with
      canned `/health` bodies. — ~50 lines.
- [x] 5.21 (RED) Test that `421` is followed exactly once with the POST body sent exactly once —
      the fake asserts on received-body count, which is the actual no-duplication check. (Spec
      `queue-client-failover` — scenario "A 421 never causes a duplicated write".) — ~45 lines.
- [x] 5.22 (RED) Test that all-slaves-reachable-no-master produces bounded backoff request count
      in a fixed window, and whole-cluster-down produces `503` with near-zero request rate over
      that window. — ~45 lines.
- [x] 5.23 (RED) Test that `/pedidos/tomar` is never sent to a fake node scripted to answer `rol:
      "slave"`. — ~25 lines.
- [x] 5.24 Verify RED: run the new file; confirm it fails only where the fake-server scaffolding
      itself has bugs, not where `ClienteReplica` (already green from 5a) is missing behaviour.
- [x] 5.25 (GREEN) Fix any fake-server scaffolding gaps surfaced by 5.20–5.23; these tests should
      mostly go green immediately given 5a is already implemented — this file is primarily an
      integration confirmation, not new production code. — ~10 lines (scaffolding fixes only).
- [x] 5.26 (REFACTOR) Ensure `test_cluster_cola.py`'s fake servers are torn down deterministically
      (no leaked threads/sockets between test cases).
- [x] 5.27 Verify: `./.venv/bin/python -m unittest discover -s tests -v` — full suite green,
      count at or above 100.

---

## Phase 6: Slice 6 — Balanceador cutover and cleanup (balanceador)

The cutover. Reversible by env var until `cola/` is removed.

### 6a — Wiring

- [ ] 6.1 (RED) Write the `BA_COLA_URL` comma-split test: a three-URL value produces a 3-element
      seed list passed to `ClienteReplica`; a single URL (no commas) produces a one-element seed
      list and operates identically to pre-change behaviour. (Spec
      `balanceador-queue-integration` — BA_COLA_URL Accepts a Comma-Separated Seed List, both
      scenarios.) — ~25 lines.
- [ ] 6.2 (RED) Write the `derivar()` never-lets-a-redirect-reach-the-caller test, and the
      exhausted-leaderless-budget-resolves-cleanly test (not a bare `502`). (Spec
      `balanceador-queue-integration` — derivar() Never Lets a Redirect Reach the Caller, both
      scenarios.) — ~35 lines.
- [ ] 6.3 (RED) Write the `recolectar()` status-handling tests: `204` does not trigger backoff
      sleep; `503`/`421` triggers `time.sleep(ESPERA_REINTENTO)` before retry; connection failure
      also triggers backoff, distinctly from `204`. (Spec `balanceador-queue-integration` —
      recolectar() Distinguishes 204, 503, 421, and Connection Failure, all three scenarios.) —
      ~40 lines.
- [ ] 6.4 (RED) Write the `salud()` additive-fields tests: a healthy cluster reports
      `cola.estado: "sana"` with the master's `rol`/`termino`; an electing cluster reports
      `cola.estado: "eligiendo"`; a fully down cluster reports `cola.estado: "caída"`; an existing
      consumer reading only pre-existing fields is unaffected. (Spec
      `balanceador-queue-integration` — salud() Reports Additive Cluster Fields, all four
      scenarios.) — ~40 lines.
- [ ] 6.5 Verify RED: run `./.venv/bin/python -m unittest discover -s tests -v`; confirm 6.1–6.4
      fail for the expected reason (`balanceador.py`/`consola.py` still on the single-URL,
      `ClienteCola` path).
- [ ] 6.6 (GREEN) `app/balanceador.py`: parse `BA_COLA_URL` into a seed list (`:73`), construct
      `ClienteReplica(URLS, ...)` in place of `ClienteCola(COLA_URL, COLA_TOKEN)` (`:288`). Run
      test 6.1 to green. — ~25 lines.
- [ ] 6.7 (GREEN) `app/balanceador.py`: rework `derivar()` (`:386-441`) so any code other than
      `202`/`503` from `ClienteReplica` (which never leaks a raw `421`) resolves to a definitive
      outcome, never a generic `502` conflating "queue rejected" with "cluster leaderless". Run
      test 6.2 to green. — ~40 lines.
- [ ] 6.8 (GREEN) `app/balanceador.py`: rework `recolectar()` (`:346-376`) to branch on `204` (no
      sleep) versus `503`/`421`/connection-failure (`time.sleep(ESPERA_REINTENTO)`). Run test 6.3
      to green. — ~30 lines.
- [ ] 6.9 (GREEN) `app/balanceador.py`: extend `salud()`/`backends_json()` (`:550-604`, `:448-477`)
      with `cola.rol`/`cola.termino`/`cola.estado`/`cola.instancias`, purely additive. Run test 6.4
      to green. — ~35 lines.
- [ ] 6.10 (GREEN) `consola.py` (`:100`, `:641`): generate a comma-separated seed list instead of a
      single URL. Add or update a `consola.py`-side test confirming the generated `BA_COLA_URL`
      lists all cluster node URLs. (Spec `balanceador-queue-integration` — consola.py Generates a
      Seed List.) — ~20 lines.
- [ ] 6.11 (REFACTOR) Confirm `ClienteCola` import is fully removed from `balanceador.py`'s
      construction path (still importable from `clientecola.py` itself, per the rollback plan);
      re-run full suite green.
- [ ] 6.12 Verify: `./.venv/bin/python -m unittest discover -s tests` reports **at or above 100
      tests passing** (spec `balanceador-queue-integration` — The Balanceador Suite Does Not
      Regress Below Its Baseline).

### 6b — Cleanup: remove the verified `cola/` duplicate

- [ ] 6.13 Re-verify `cola/` has not drifted since exploration.md's confirmed byte-identical diff:
      run `diff -rq sdypp_balanceador/cola sdypp_colas_serv/colas.py sdypp_colas_serv/servidor.py
      sdypp_colas_serv/Dockerfile` **is not the correct comparison at this point** — `colas_serv`
      has evolved through slices 2-4 (Raft additions) and is no longer byte-identical to the frozen
      `cola/`. Instead: confirm via `git log -- cola/` in `sdypp_balanceador` that `cola/` has
      received **zero commits** since the subtree split (it was never touched by tasks 1.x–6.12),
      which is what makes it safe to delete as dead, unused, pre-Raft code rather than a
      still-diverging duplicate. — read-only verification, no lines changed.
- [ ] 6.14 Confirm `cola/` is unreferenced: `grep -rn "^from cola\|^import cola\|from \.cola\b"
      sdypp_balanceador/app sdypp_balanceador/*.py` returns nothing — no remaining import anywhere
      in the balanceador codebase targets `cola/` (it was already superseded operationally by
      `ClienteReplica` talking to the standalone `sdypp_colas_serv` service in 6a). — read-only
      verification.
- [ ] 6.15 Delete `sdypp_balanceador/cola/` (all files: `colas.py`, `servidor.py`, `Dockerfile`,
      `README.md`, and any `__init__.py`). — this is a pure deletion, estimated ~900-1100 lines
      removed; **candidate for `size:exception`** given it introduces zero new logic to review.
- [ ] 6.16 Delete `sdypp_balanceador/tests/test_colas.py` and
      `sdypp_balanceador/tests/test_servidor_cola.py` (both already relocated and evolved inside
      `sdypp_colas_serv/tests/` since task 2.1's window; see Phase 4's relocation note). —
      estimated ~300-400 lines removed.
- [ ] 6.17 Verify: `./.venv/bin/python -m unittest discover -s tests -v` — suite still reports at
      or above 100 passing tests with `cola/` and its tests gone; no `ModuleNotFoundError` for
      `cola.*` anywhere in the run.
- [ ] 6.18 Update `docs/plan-cola-desacoplada.md` (or a short changelog note) to record that
      Etapa 3 (repository split) is now fully closed — the duplicate is gone, not merely
      byte-identical.

---

## Cross-cutting

- [ ] 7.1 After every slice merges (per the chain in the forecast table), confirm the
      corresponding proposal Success Criteria checkbox(es) are demonstrably true before starting
      the next slice's RED tasks — do not defer verification to the end of the whole change.
- [ ] 7.2 At slice 6's completion, run the proposal's manual E2E chaos checklist once (3-node local
      cluster; kill master with in-flight `202`s; verify zero loss; kill+revive a slave; verify a
      revived zombie master steps down and commits nothing) and capture the evidence for the
      informe, per the Testing Strategy's "E2E — manual, for the informe" row. Not part of CI.
