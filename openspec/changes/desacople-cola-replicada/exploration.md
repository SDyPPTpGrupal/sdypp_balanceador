# Exploration: Decouple the queue system into a replicated standalone service (Raft-lite master/slave)

Change: `desacople-cola-replicada`
Date: 2026-09-20
Status: complete (open product decisions listed at the end)

Source documents (authoritative):
- `docs/plan-cola-desacoplada.md` — the full plan.
- `docs/contrato-worker.md` — the frozen v1 worker contract handed to another team.

## Current State

### Repository split (Option B) is done and clean

`sdypp_colas_serv/{colas.py,servidor.py,Dockerfile,README.md}` is a `git subtree split` of
`sdypp_balanceador/cola/`. A full `diff` of `colas.py`, `servidor.py` and `Dockerfile` between the
two repositories reports **no differences**. The drift risk raised during exploration is closed.

### `cola/colas.py` (identical to `sdypp_colas_serv/colas.py`)

- `Pedido` dataclass (`colas.py:38-80`): carries `vence_en` (monotonic deadline) and
  `reservado_hasta`. `como_json()` (`colas.py:69-80`) strips absolute time and exposes only
  `quedaMs`.
- `ColaPedidos` (`colas.py:83-276`): `_esperando` deque (`:97`), `_en_vuelo` dict by id (`:98`),
  `_a_fallar` deque drained by the recoverer (`:99`). A single `threading.Condition` (`_hay`, `:100`)
  guards all three.
  - `publicar()` (`:110-123`): non-blocking; returns `False` when `len(_esperando) >= cota`.
  - `tomar()` (`:127-149`): long-poll pop-and-reserve;
    `reservado_hasta = min(ahora + reserva, pedido.vence_en)`.
  - `devolver()` (`:151-167`): explicit release, re-inserted at the front (`appendleft`).
  - `completar()` (`:169-190`): pops by id from `_en_vuelo` **or** from a still-waiting `_esperando`
    (the late-answer case, after the recoverer already re-queued it); returns `None` on an unknown
    id, which is the second-answer case.
  - `recuperar()` (`:194-246`): lazy expiry, invoked every `COLA_INTERVALO_RECUPERADOR`
    (`servidor.py:92`, default 0.25s). Expired `_en_vuelo` entries are re-queued when `idempotente`
    and `queda() > 0`, otherwise failed with `DEADLINE_EXCEEDED`. Also drops `_esperando` entries
    whose `queda() <= 0`.
  - `_proximo_vivo()` (`:267-276`): a **second, synchronous** lazy-expiry path, evaluated inside
    every `tomar()` call rather than only by the periodic sweep.
- `ColaRespuestas` (`:279-356`): dict keyed by `destinatario`, each holding a deque of
  `(instante, respuesta)`. `purgar()` (`:333-345`) is the TTL-based lazy expiry, driven by the same
  recoverer loop.
- `Sistema` (`:359-465`): `responder()` (`:385-408`) and `recuperar()` (`:410-424`) are each
  **compound two-mutation operations** (pop from pedidos + publish to respuestas). This matters for
  how a single committed Raft log entry must apply.

### `cola/servidor.py` (identical to `sdypp_colas_serv/servidor.py`)

- A single shared `TOKEN` (`:99`, from `COLA_TOKEN`). The plan's split into
  `COLA_TOKEN_PUBLICADOR` / `COLA_TOKEN_CONSUMIDOR` / `COLA_TOKEN_CLUSTER` is pending work, not
  drift.
- The `recuperador()` thread (`:131-150`) is the sole per-node owner of periodic lazy expiry, driven
  by that node's own `time.monotonic()`. This is exactly the mechanism the plan requires to move
  behind the replication log.
- Routes wired in `do_POST` / `do_GET` (`:336-369`): `/pedidos`, `/pedidos/tomar`,
  `/pedidos/devolver`, `/respuestas`, `/respuestas/tomar`, `/health`, `/estado`. No `/raft/*` yet.
- `/health` (`:313-326`) carries no role, term, or known-master fields today.

### `app/clientecola.py` — the surface `ClienteReplica` must preserve

- `__init__(self, url, token="", timeout=5.0, conexiones=8)` (`:29`) takes a **single** URL.
  `ClienteReplica(urls, ...)` is therefore a genuine constructor-signature change, not a rename.
- Consumed surface: `publicar_pedido(pedido)` (`:42-44`),
  `tomar_respuesta(destinatario, espera)` (`:46-56`), `estado()` (`:58-64`).
- `ErrorCola` (`:22-24`) already distinguishes "unreachable" from "answered with an error". That
  exact split must drive the new client's "connection failure -> re-probe master" versus
  "409 no-soy-master -> follow the redirect without duplicating the POST".
- `_pedir()`'s pool retry (`:87-132`) retries only a **reused, stale** connection, never a fresh
  one. Preserving this is what keeps an outer master-redirect layer from blindly re-sending a POST
  of unknown delivery status.

### `app/balanceador.py`

- `derivar()` (`:386-441`): builds the pedido, calls `CLIENTE.publicar_pedido()`, registers an
  `Espera` in `ESPERAS` (`:342-343`), and blocks on
  `espera.listo.wait(timeout=PRESUPUESTO + GRACIA)` (`:423`). Today any code other than 202/503
  falls into a generic `502 "la cola rechazó el pedido"` (`:419-421`) — wrong for a future master
  redirect, which confirms that 409-following must live entirely inside `ClienteReplica` and never
  reach `derivar()`.
- `recolectar()` (`:346-376`): loops on `CLIENTE.tomar_respuesta(IDENTIDAD, ESPERA_RECOLECTOR)` and
  matches by `id`. `ClienteCola.tomar_respuesta()` **already collapses every non-200 into `None`**
  (`:46-56`), so 204, 503 and 409 are indistinguishable at this layer today. `ClienteReplica` must
  split them apart so that `409 no-soy-master` triggers an immediate re-probe instead of a silent
  no-op that would hammer a stale node.
- `salud()` / `backends_json()` (`:550-604`, `:448-477`) consume the existing shape of
  `CLIENTE.estado()`. The plan's added `cola.rol` / `cola.termino` / `cola.instancias` fields are
  additive and low risk.

### Correction to the plan: `BA_COLA_URL` is not comma-separated today

The plan states that "`BA_COLA_URL` sigue aceptando una lista separada por comas". Verified against
the code, this is false: `balanceador.py:73` reads a single value
(`COLA_URL = os.environ.get("BA_COLA_URL", "http://127.0.0.1:8085")`) and passes it straight into
`ClienteCola(COLA_URL, COLA_TOKEN)` (`:288`); `consola.py:100` and `consola.py:641` also generate a
single URL. Comma-splitting is **new work**, not existing behaviour being preserved.

## Affected Areas

- `sdypp_colas_serv/colas.py` — business logic unchanged per the plan, but `publicar` / `tomar` /
  `devolver` / `completar` (`responder`) and both lazy-expiry sweeps are precisely the mutation
  boundary the log must wrap.
- `sdypp_colas_serv/servidor.py` — role and term state, `/raft/*` routes, split tokens, and the
  wrong-node redirect on every data route.
- `sdypp_colas_serv/raft.py` (new) — election, heartbeats, replication, term fencing.
- `app/clientecola.py` -> new `app/clientereplica.py` (`ClienteReplica`) — constructor signature
  change, plus new body-based redirect routing.
- `app/balanceador.py` — `derivar()` / `recolectar()` need the 204 / 409 / `ErrorCola` distinctions
  above; `salud()` gains additive fields.
- `tests/test_colas.py`, `tests/test_servidor_cola.py` — the plan says unchanged, but
  `test_servidor_cola.py` speaks HTTP to `servidor.py`. Because the worker contract promises that a
  one-element seed list keeps working, a single node needs an explicit "trivially always master"
  mode rather than an unstated assumption.
- New: `sdypp_colas_serv/tests/test_raft.py`, `tests/test_cluster_cola.py`.

## Approaches Considered

1. **Follow the plan exactly — hand-rolled Raft-lite, standard library only.**
   Pros: already specified with its correctness reasoning; satisfies the no-external-consensus
   constraint; the worker contract is frozen separately and does not block it.
   Cons: election safety, log matching, commit-index advancement and rejoin catch-up are genuinely
   hard to get right and to test without flakiness. Effort: high.
2. **Primary plus synchronous mirror (two-node hot standby, no election).**
   Pros: much less code, far fewer races; still avoids losing acknowledged writes if the standby
   must ack. Cons: explicitly rejected by the plan's stated priority (minimum three nodes, odd, with
   majority tie-break); weaker partition tolerance; re-opens a settled team decision. Effort:
   medium.
3. **Single node plus disk checkpoint and replay.**
   Pros: trivial next to Raft; survives a process crash. Cons: does not survive an unreachable node
   or machine, which is the actual goal. Listed only as a rejected alternative. Effort: low, but it
   does not meet the requirement.

**Recommendation: approach 1.** This is already a team decision — the plan explicitly rejects the
partition alternative — and is not re-opened here. The real risk is not the choice of approach but
getting Raft-lite correct and deterministically testable with the standard library alone under
Strict TDD.

## Hard Parts, Gaps and Inconsistencies

1. **The `409 no-soy-master` versus `409 desconocido` collision — confirmed, unresolved.**
   `servidor.py:279-298` (`publicar_respuesta`) already emits `409 {"resultado":"desconocido"}` and
   `409 {"resultado":"destinatario-saturado"}` on the same `/respuestas` route that would gain a
   third 409 shape once clustering lands. "Distinguish by body, not by status" works, but is fragile
   against any naive `status == 409` check. Both source documents flag `421 Misdirected Request` as
   the proposed fix and mark the decision as pending. It must be settled before the wire contract is
   frozen in `sdd-spec`.

2. **Replicating lazy expiry through the log — confirmed, and the plan leaves a real gap.**
   `_proximo_vivo()` (`colas.py:267-276`) resolves expiry synchronously inside `tomar()` using the
   local node's clock. Once expiry must be a log-committed decision, `tomar()` can no longer settle
   this on its own. Either (a) treat a locally-detected but uncommitted expiry as not-yet-expired,
   risking that the master hands out an obviously dead entry while its expiry record is still in
   flight, or (b) block `tomar()` until that expiry entry commits, adding latency to every call that
   meets an expired head-of-queue item. The plan chooses neither.

3. **Compound mutations versus the single-entry operation vocabulary.** The plan's operations
   (`encolar`, `tomar`, `devolver`, `responder`, `retirar-respuesta`, `expirar`) treat `responder` as
   one log entry, yet `Sistema.responder()` and `Sistema.recuperar()` each perform two mutations. The
   plan never states that the apply step calls those `Sistema` methods verbatim, leaving the
   `raft.py` to `colas.py` wiring point ambiguous.

4. **`recuperador()` starts unconditionally regardless of role** (`servidor.py:131-150`). It must be
   gated to the master. Beyond that — and unmentioned in the plan — a node that has just won an
   election needs an immediate one-shot recovery pass before serving new `tomar()` calls, because
   reservations may have expired during the leaderless window. Without it, a freshly elected master
   can hand out an already-dead pedido.

5. **Client constructor API break.** `ClienteCola(url)` to `ClienteReplica(urls)`, compounded by the
   `BA_COLA_URL` correction above: the comma-separated list has to be built, not preserved.

## Testability Under Strict TDD (standard library only)

- **Unit level:** `raft.py`'s state machine — term increment, the vote-granting rule, the
  "at least as up to date" log comparison, and commit-index advancement from a fixed set of follower
  ack indices — should be testable as pure functions or small classes with a fake clock, needing
  neither sockets nor real timers. This is the highest-priority seam, because it is where election
  safety and log-matching bugs hide.
- **Integration level:** a three-node cluster test can start three `ThreadingHTTPServer` instances on
  `127.0.0.1` with free ports in-process, which is the pattern `tests/test_servidor_cola.py` already
  uses. Use short, deterministic election and heartbeat timeouts with seeded or disabled jitter.
- **Chaos scenarios:** "kill" means calling `shutdown()` on that node's `ThreadingHTTPServer`, a
  lifecycle already exercised by the existing tests; "partition" means a fake transport that drops
  connections between specific peers. Both stay within standard library plus `unittest`.
- **`ClienteReplica` tests:** the plan's `tests/test_cluster_cola.py` approach — minimal
  `http.server` handlers returning scripted `409` and master bodies — is the right seam, because it
  tests the client against canned responses with no real Raft underneath.
- **Determinism risk:** election timeout plus jitter is inherently timing-sensitive. Prefer an
  injectable clock or an explicit "force election now" test hook over relying on wall-clock races.
- `sdypp_colas_serv/tests/` does not exist yet. Under Strict TDD, `test_raft.py` — majority election,
  stale-log rejection, term fencing, no double master — must be written RED-first, before `raft.py`.

## Risks

- One product decision (the 409/421 redirect signal) blocks a clean wire-contract freeze.
- The post-election catch-up gap (item 4) is a correctness hole absent from the plan; left
  unaddressed it silently hands out dead pedidos right after a failover.
- Election-timing tests are the most likely source of future flakiness; the injectable-clock seam is
  not optional.

## Open Product Decision

**`409 no-soy-master` versus `421 Misdirected Request`** for the wrong-node redirect signal. Flagged
as pending in both source documents, and owned jointly with the team writing the worker. Needed
before the wire contract is frozen in `sdd-spec`.

Two further questions raised during exploration are design decisions rather than product decisions
and are resolved in `sdd-design`, not by the user: how `tomar()` behaves against a locally detected
but uncommitted expiry (item 2), and whether post-election catch-up recovery is in scope (item 4 —
treated as in scope, because it is a correctness hole rather than an enhancement).
