# Queue Client Failover Specification

## Purpose

Defines `ClienteReplica`, the balanceador-side client that talks to the queue cluster:
seed-list-based cold-start discovery, master caching, one-shot `421` redirect following
without duplicating the original POST, backoff while the cluster is leaderless, and the
connection-failure-versus-wrong-node distinction. `ClienteReplica` replaces
`ClienteCola`'s single-URL constructor; this is a genuine constructor-signature change, not
a compatible extension.

## Requirements

### Requirement: Constructor Accepts a Seed List

`ClienteReplica` MUST be constructed as `ClienteReplica(urls, token="", timeout=5.0,
conexiones=8)`, where `urls` is a non-empty list of one or more node URLs (the seed list).
A single-element list MUST be a fully supported configuration, not a degraded one.

#### Scenario: A single-element seed list is valid

- GIVEN `ClienteReplica(["http://127.0.0.1:8085"], token="t")`
- WHEN the client is used to publish a pedido or pull a response
- THEN it MUST operate correctly against that one node, treating it as the (trivial) master

### Requirement: Cold-Start Master Discovery

On first use with no cached master, `ClienteReplica` MUST probe the seed list via `GET
/health` (no token) until it finds a node reporting `"rol": "master"`, or a node reporting a
non-null `"masterConocido"`, and MUST cache that URL as the current master. If every probed
node is unreachable or reports no known master (`"masterConocido": null`, mid-election), the
client MUST have no master to use and MUST fall back to the backoff behavior described
below rather than guessing one.

#### Scenario: Cold start finds the master directly

- GIVEN a 3-node seed list where the first probed node itself reports `"rol": "master"`
- WHEN `ClienteReplica` performs cold-start discovery
- THEN it MUST cache that node's URL as the master without probing the remaining nodes

#### Scenario: Cold start follows masterConocido from a slave

- GIVEN a 3-node seed list where the first probed node is a slave reporting
  `"masterConocido": "<url B>"`
- WHEN `ClienteReplica` performs cold-start discovery
- THEN it MUST cache `<url B>` as the master

#### Scenario: Cold start during an election finds nothing to cache

- GIVEN every node in the seed list reports `"masterConocido": null` (an election is in
  progress)
- WHEN `ClienteReplica` performs cold-start discovery
- THEN it MUST NOT cache any URL as master and MUST proceed to backoff-and-retry rather than
  failing immediately or guessing a node

### Requirement: Cached Master Used at Zero Discovery Cost in Steady State

Once a master URL is cached, every subsequent operation (`publicar_pedido`,
`tomar_respuesta`, publish/pull calls) MUST be sent directly to the cached master with no
preceding `/health` probe. Discovery MUST only be triggered by a connection failure or a
`421` response, not before every operation.

#### Scenario: Steady-state operation issues no extra health probes

- GIVEN a cached master URL from a prior successful operation
- WHEN 100 consecutive `tomar_respuesta` calls succeed against that master
- THEN none of those calls MUST be preceded by a `GET /health` probe

### Requirement: One-Shot Redirect Following Without Duplicating the POST

When an operation against the cached master returns `421 {"error": "no-soy-master",
"master": <url-or-null>}`, `ClienteReplica` MUST update its cached master to the returned
`master` value (which may be `null`) and MUST retry the *same logical operation exactly
once* at the new location — it MUST NOT resend a POST whose delivery status is unknown
(i.e., it MUST NOT retry a request that may have already reached and mutated a previous
node's state; a `421` is safe to retry because it is a definitive "not processed" response).
If the retried request again returns `421`, the client MUST NOT loop indefinitely following
redirects; it MUST fall back to full seed-list discovery with backoff.

#### Scenario: A stale cache follows exactly one redirect and succeeds

- GIVEN a cached master URL that is now stale (that node stepped down to slave)
- WHEN `ClienteReplica` posts an operation to the stale cached URL and receives `421` with
  `"master": "<new url>"`
- THEN it MUST retry the identical operation against `<new url>` exactly once
- AND if that retry succeeds, it MUST return that result without any further redirect
  attempts

#### Scenario: A redirect chain does not loop forever

- GIVEN a pathological case where the redirected-to node also returns `421`
- WHEN `ClienteReplica` receives a second `421` for the same logical operation
- THEN it MUST NOT follow a second redirect in the same attempt; it MUST fall back to
  full seed-list discovery with backoff instead of retrying indefinitely

#### Scenario: A 421 never causes a duplicated write

- GIVEN a `POST /pedidos` that received `421` (meaning it was never processed by that node)
- WHEN the client retries against the redirect target
- THEN exactly one attempt to actually process that pedido MUST reach the eventual master,
  because the node that answered `421` is defined to not have applied it

### Requirement: A Redirect Target Outside the Seed List Is Rejected

`ClienteReplica` MUST validate the `master` URL carried by a `421` against its static seed
list before caching it or sending any request to it. A target that is not a member of the
configured seed list MUST be ignored and treated exactly as `master: null` — the client MUST
NOT cache it, MUST NOT send any request to it, and MUST fall back to seed-list discovery with
backoff.

The seed list is configuration, not data received over the network, which is what makes it a
trustworthy bound. Without this rule a single misbehaving or spoofed node could redirect all
balanceador traffic — pedidos and responses both — to a host of its choosing.

#### Scenario: A redirect to an unknown host is never contacted

- GIVEN a seed list of `http://cola-1:8085`, `http://cola-2:8086`, `http://cola-3:8087`
- WHEN a node answers `421 {"error": "no-soy-master", "master": "http://evil:8085"}`
- THEN `ClienteReplica` MUST NOT send any request to `http://evil:8085`
- AND it MUST NOT cache that URL as the master
- AND it MUST treat the response as if `master` were `null` and fall back to seed-list
  discovery with backoff

#### Scenario: A redirect to a seed-list member is followed normally

- GIVEN the same seed list
- WHEN a node answers `421` with `"master": "http://cola-2:8086"`
- THEN `ClienteReplica` MUST cache that URL and retry the operation there exactly once

### Requirement: Connection Failure Triggers Re-Discovery, Never a Blind Retry of a Fresh Request

A connection failure (refused, reset, or timeout) while contacting the cached master MUST
invalidate the cache (set it to unknown) and trigger a fresh discovery probe of the seed
list, following the same connection-failure semantics `ErrorCola` already distinguishes in
`clientecola.py`. A fresh request that failed via connection error MUST NOT be blindly
retried unless the existing pool-retry rule (retry only a reused, stale connection, never a
freshly opened one) applies — this is what prevents a redirect layer from re-sending a POST
of unknown delivery status.

#### Scenario: A connection reset invalidates the cache and re-discovers

- GIVEN a cached master that becomes unreachable (process killed)
- WHEN `ClienteReplica` attempts an operation against it and gets a connection error
- THEN it MUST clear the cached master and re-run seed-list discovery, distinct from how it
  handles a `421` response

#### Scenario: A fresh connection's failure is not blindly retried

- GIVEN a freshly opened connection to the master that fails while sending a `POST
  /pedidos` request whose delivery status is therefore unknown
- WHEN `ClienteReplica` observes that failure
- THEN it MUST NOT automatically resend that same POST on a new connection; it MUST
  surface the failure the same way `ErrorCola` already does today, distinguishing this case
  from a stale-pooled-connection retry

### Requirement: Backoff While the Cluster Is Leaderless

While no node in the seed list reports being master (`masterConocido` is `null`
everywhere, or every node is unreachable), `ClienteReplica` MUST retry discovery with
backoff rather than in a closed loop, and MUST give up and surface `503 {"error": "el
sistema de colas no responde"}` once the operation's budget (`presupuestoMs` or equivalent
caller-supplied budget) is exhausted.

#### Scenario: A leaderless cluster does not spin the CPU

- GIVEN every node in the seed list currently reports no known master
- WHEN `ClienteReplica` retries discovery for several seconds
- THEN it MUST sleep between attempts (backoff), not busy-loop, keeping caller CPU usage
  near zero during that window

#### Scenario: Exhausted budget surfaces 503

- GIVEN a caller-supplied budget that elapses while the cluster remains leaderless
- WHEN `ClienteReplica`'s backoff-and-retry loop exceeds that budget without ever finding a
  master
- THEN it MUST return/raise the equivalent of `503 {"error": "el sistema de colas no
  responde"}` rather than continuing to retry unboundedly

### Requirement: Never Writes to a Known Slave

`ClienteReplica` MUST NOT deliberately send a mutating data-route request (`/pedidos`,
`/pedidos/tomar`, `/pedidos/devolver`, `/respuestas`) to a node it has learned, via a prior
`/health` probe or a prior `421`, currently reports role `"slave"`.

#### Scenario: A node known to be a slave is never targeted for a write

- GIVEN a `/health` probe that reported a node as `"rol": "slave"`
- WHEN `ClienteReplica` next needs to send `/pedidos/tomar`
- THEN it MUST NOT target that node directly for the write; it MUST use its cached master
  or run discovery first
