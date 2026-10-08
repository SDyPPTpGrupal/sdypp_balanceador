# Queue Service API Specification

## Purpose

Defines the HTTP surface of a `sdypp_colas_serv` node: the data routes consumed by the
balanceador and the worker, the `421` wrong-node redirect, the `/raft/*` cluster-internal
routes, the three-way token authorization split, `/health` and `/health/vivo`, the declared
contract version, and single-node mode. This is the wire contract — request/response shapes
and status codes — independent of how entries are replicated (`queue-replication`) or
applied to state (`queue-log-application`).

No absolute time instant crosses this wire in either direction. Every time-remaining value
is expressed as `quedaMs`, computed by the node serving the request against its own clock,
because client and server clocks are not synchronized.

## Requirements

### Requirement: Contract Version Declaration Stays 1.0

`GET /health` MUST declare `"contrato": "1.0"`. Per the existing versioning rule (adding an
optional field is a v1-compatible change; removing/renaming a field, changing what a status
code means, or making an optional thing mandatory is a v2 event), replacing the documented
`409 {"error": "no-soy-master"}` behavior with `421 Misdirected Request` MUST NOT bump the
contract's major version, for the following reason, which MUST be recorded as the rationale
wherever the version is discussed: no deployed `sdypp_colas_serv` node has ever emitted
`409 {"error": "no-soy-master"}` — the pre-change single-node deployment is, by construction,
always its own master and never emits it, and `docs/contrato-worker.md` states as much
("nunca lo devuelve") as the documented state of the running system, not merely a claim
about a future one. Therefore `421` is not a redefinition of a status code's meaning to any
consumer that ever observed `409` in this context; it is the introduction of a new code for
a case that has never actually occurred on the wire. The two existing `409` meanings on
`/respuestas` (`desconocido`, `destinatario-saturado`) are unchanged and MUST continue to be
emitted exactly as before.

#### Scenario: A worker built against the v1 contract handles the new redirect without a version bump

- GIVEN a worker implementation coded against `docs/contrato-worker.md` v1, which already
  advises branching on response body rather than raw status code
- WHEN the worker is pointed at a multi-node cluster (a scenario the pre-change single-node
  deployment could never produce) and receives `421 {"error": "no-soy-master", "master":
  "<url>"}`
- THEN the worker's declared major contract version check against `GET /health`'s
  `"contrato": "1.0"` MUST still pass, because no field was removed or renamed and no
  previously-emitted status code changed meaning

#### Scenario: The two /respuestas 409 meanings are unaffected

- GIVEN a master node handling `POST /respuestas`
- WHEN the pedido was already answered by another delivery
- THEN it MUST still reply `409 {"resultado": "desconocido"}`
- WHEN the balanceador is not currently collecting for that destinatario
- THEN it MUST still reply `409 {"resultado": "destinatario-saturado"}`
- AND neither of these two bodies MUST ever be confused with the wrong-node case, which now
  uses a distinct status code (`421`) rather than sharing `409`

### Requirement: Wrong-Node Redirect on Every Data Route

Any node that is not the current master MUST reply `421 Misdirected Request` to any of
`POST /pedidos`, `POST /pedidos/tomar`, `POST /pedidos/devolver`, `POST /respuestas`, and
`POST /respuestas/tomar`, with body `{"error": "no-soy-master", "master": "<url>"}` when a
master is currently known, or `{"error": "no-soy-master", "master": null}` when the cluster
has no master (an election is in progress). A node MUST NOT apply any mutation, and MUST
NOT count any request toward a majority commit, before replying `421` in this case.

#### Scenario: A slave rejects a write with 421 and a known master

- GIVEN a 3-node cluster with node A as master and node B as slave, both aware of each
  other via heartbeats
- WHEN a client sends `POST /pedidos` to node B
- THEN node B MUST reply `421` with body `{"error": "no-soy-master", "master": "<A's URL>"}`
- AND node B MUST NOT apply the pedido to any local state

#### Scenario: A node rejects a write with 421 and a null master during an election

- GIVEN a cluster with no node currently in the `master` role (an election is underway)
- WHEN a client sends any data-route request to any node
- THEN that node MUST reply `421` with body `{"error": "no-soy-master", "master": null}`

#### Scenario: 421 never appears on a non-data route

- GIVEN a slave node
- WHEN a client calls `GET /health`, `GET /raft/estado`, `POST /raft/appendEntries`, or
  `POST /raft/requestVote`
- THEN the node MUST answer normally and MUST NOT reply `421`, because `421` is scoped to
  the five data routes only

### Requirement: POST /pedidos — Publish a Pedido

The master MUST accept `POST /pedidos` with body `{id?, operacion, parametros,
idempotente?, destinatario, cliente?, presupuestoMs}`, where `id` is optional (server-
generated if absent) and `idempotente` defaults to `false`. The master MUST NOT reply `202`
until a majority of the cluster holds the corresponding log entry. When the queue is at
capacity, the master MUST reply `503 {"error": "cola llena", "esperando": <n>, "cota":
<n>}`.

#### Scenario: A pedido is only acknowledged after majority commit

- GIVEN a 3-node cluster (1 master, 2 slaves)
- WHEN a client posts `POST /pedidos` with a valid body and only the master itself holds the
  entry (no slave has acknowledged yet)
- THEN the master MUST NOT respond `202` until at least 1 of the 2 slaves acknowledges the
  entry

#### Scenario: A full queue rejects new pedidos

- GIVEN the master's queue already holds `cota` waiting pedidos
- WHEN a client posts a new `POST /pedidos`
- THEN the master MUST reply `503 {"error": "cola llena", "esperando": <cota>, "cota":
  <cota>}` and MUST NOT append a new log entry for it

### Requirement: POST /pedidos/tomar — Long-Poll Pull

The master MUST accept `POST /pedidos/tomar` with body `{consumidor, espera}` (long-poll,
`espera` seconds, capped at 30) and MUST reply one of: `200` with
`{id, operacion, parametros, idempotente, cliente, quedaMs, intento}` when a pedido is
available; `204` with no body when no pedido became available within `espera` seconds; or
`421` when not master. `quedaMs` MUST be computed from the master's own clock at the moment
of serving and MUST NOT be, or be derived from, any absolute timestamp sent by another
node or by the client.

#### Scenario: A pedido is handed out with a fresh quedaMs

- GIVEN a pedido was published 3 seconds ago with `presupuestoMs: 5000`
- WHEN a worker calls `POST /pedidos/tomar` against the master and a pedido becomes
  available
- THEN the response MUST include `quedaMs` close to `2000`, computed from the master's own
  clock, not from any client- or peer-supplied instant

#### Scenario: Long-poll times out with 204

- GIVEN an empty queue
- WHEN a worker calls `POST /pedidos/tomar` with `espera: 5` and no pedido arrives within 5
  seconds
- THEN the master MUST reply `204` with no body

### Requirement: POST /respuestas — Publish a Response

The master MUST accept `POST /respuestas` with body `{id, estado, contenido, atendidoPor,
app}`. `destinatario` MUST NOT be accepted from the caller; the master MUST derive it from
the stored pedido. The master MUST reply one of: `202 {"resultado": "entregada"}` after
majority commit; `409 {"resultado": "desconocido"}` when the pedido was already answered;
`409 {"resultado": "destinatario-saturado"}` when the balanceador is not currently
collecting; or `421` when not master.

#### Scenario: Second response to the same pedido is rejected

- GIVEN a pedido whose response was already delivered
- WHEN a second worker posts `POST /respuestas` for the same `id`
- THEN the master MUST reply `409 {"resultado": "desconocido"}` and MUST NOT deliver a
  second response for that pedido

#### Scenario: destinatario is ignored if sent by the caller

- GIVEN a worker that mistakenly includes a `destinatario` field in its `POST /respuestas`
  body
- WHEN the master processes the request
- THEN the master MUST use the `destinatario` it stored from the original pedido, MUST
  ignore any caller-supplied value, and MUST NOT let a response be routed based on a
  caller-supplied destinatario

### Requirement: POST /pedidos/devolver — Release a Reservation

The master MUST accept `POST /pedidos/devolver` with body `{id, consumidor}` and MUST reply
`200 {"resultado": "devuelto"}` on success, `409 {"resultado": "no-estaba-en-vuelo"}` when
the reservation already expired or does not exist, or `421` when not master.

#### Scenario: Releasing an already-expired reservation is a no-op error

- GIVEN a pedido whose reservation already expired and was reclaimed by the recoverer
- WHEN the original consumer calls `POST /pedidos/devolver` for that pedido's id
- THEN the master MUST reply `409 {"resultado": "no-estaba-en-vuelo"}` and MUST NOT mutate
  any state as a result

### Requirement: POST /respuestas/tomar — Long-Poll Response Pull

The master MUST accept `POST /respuestas/tomar` with body `{destinatario, espera}`
(long-poll) and MUST reply `200` with `{id, operacion, estado, contenido, atendidoPor, app,
intentos, esperaMs}` when a response is available, `204` with no body when the poll window
elapses with no response for that destinatario, or `421` when not master.

The `204` is the existing behaviour and MUST be preserved: `servidor.py:300-308` already
replies `204` on an empty pull, matching `POST /pedidos/tomar` (`servidor.py:255`). A `204`
MUST carry no body — `servidor.py:170-177` documents why, and it is not a style preference:
HTTP clients know a `204` has no body and do not read one from the socket, so stray bytes are
left in the buffer and surface as a `BadStatusLine` on the *next*, unrelated request.

#### Scenario: A collector receives a response addressed to it

- GIVEN a response was published for `destinatario: "balanceador@casa-tomas"`
- WHEN that destinatario calls `POST /respuestas/tomar`
- THEN it MUST receive that response's full shape including `intentos` and `esperaMs`

#### Scenario: A collector polls with nothing waiting for it

- GIVEN no response has been published for `destinatario: "balanceador@casa-tomas"`
- WHEN that destinatario calls `POST /respuestas/tomar` with `espera`
- THEN the master MUST hold the connection for up to `espera` seconds
- AND MUST then reply `204` with an empty body
- AND the caller MUST treat that `204` as "nothing yet", distinct from `421` and from a
  connection failure

### Requirement: Cluster-Internal Routes

Every node MUST expose `POST /raft/appendEntries`, `POST /raft/requestVote`, and
`GET /raft/estado` (debug: role, term, log index, commit index). These routes are
cluster-internal, not part of the public task contract, and MUST require
`COLA_TOKEN_CLUSTER`, distinct from the publisher and consumer tokens. A request to any
`/raft/*` route without a valid `COLA_TOKEN_CLUSTER` MUST be rejected with `403
{"error": "token inválido"}` and MUST NOT be processed as a valid Raft message.

#### Scenario: An outside caller cannot join the cluster protocol

- GIVEN a node presenting `COLA_TOKEN_CONSUMIDOR` instead of `COLA_TOKEN_CLUSTER`
- WHEN it calls `POST /raft/requestVote`
- THEN the target node MUST reply `403 {"error": "token inválido"}` and MUST NOT grant or
  refuse a vote, because the request was never authenticated as a cluster peer

### Requirement: Three-Way Token Authorization Split

The service MUST authorize requests with three distinct tokens: `COLA_TOKEN_PUBLICADOR`
(authorizes `POST /pedidos`), `COLA_TOKEN_CONSUMIDOR` (authorizes `POST /pedidos/tomar`,
`POST /pedidos/devolver`, `POST /respuestas`), and `COLA_TOKEN_CLUSTER` (authorizes
`/raft/*`). Every authenticated route MUST require the header `X-Cola-Token` except
`/raft/*`'s own header if different from the data-route header name; a missing or invalid
token for the route being called MUST reply `403 {"error": "token inválido"}`. `GET
/health` and `GET /health/vivo` MUST NOT require a token.

#### Scenario: A consumer token cannot publish

- GIVEN a caller presenting a valid `COLA_TOKEN_CONSUMIDOR`
- WHEN it calls `POST /pedidos` (a publisher-only route)
- THEN the node MUST reply `403 {"error": "token inválido"}`

#### Scenario: Health routes require no token

- GIVEN any caller, authenticated or not
- WHEN it calls `GET /health` or `GET /health/vivo`
- THEN the node MUST respond without checking any token

### Requirement: /health Reports Cluster and Contract State

`GET /health` MUST require no token and MUST return, at minimum: `"cola"` (status string),
`"rol"` (`"master" | "slave" | "candidato"`), `"termino"` (current term), `"masterConocido"`
(the known master's URL, or `null` if an election is in progress), `"contrato"` (declared
contract version), `"instancia"` (node identity), and the existing queue depth fields
(`"esperando"`, `"enVuelo"`, `"cota"`).

#### Scenario: A slave reports the master it knows about

- GIVEN a slave node that has received a heartbeat from the current master this term
- WHEN a caller requests `GET /health`
- THEN the response MUST include `"rol": "slave"` and `"masterConocido"` set to the
  master's URL

#### Scenario: An electing cluster reports no known master

- GIVEN a node with no current master recognized (mid-election)
- WHEN a caller requests `GET /health`
- THEN `"masterConocido"` MUST be `null`

### Requirement: /health/vivo Liveness Endpoint

`GET /health/vivo` MUST be a separate route from `GET /health`, MUST require no token, and
MUST reply `200` as long as the process is alive and serving HTTP, independent of cluster
role, term, or master knowledge. It MUST be the endpoint used by the container
`HEALTHCHECK`.

#### Scenario: A leaderless node still reports alive

- GIVEN a node that is a `candidato` with no known master
- WHEN a caller requests `GET /health/vivo`
- THEN the node MUST reply `200`, because process liveness is independent of cluster
  leadership state

### Requirement: Single-Node Mode

A node started with a seed list of exactly one element (itself) MUST trivially declare
itself `master` (a majority of 1), MUST NOT run an election protocol, and MUST NOT ever
emit `421` on any data route. Its HTTP behavior on data routes MUST be byte-for-byte
identical to the pre-clustering single-node service.

#### Scenario: A lone node never redirects

- GIVEN a node configured with a seed list containing only its own URL
- WHEN a client calls any of the five data routes
- THEN the node MUST process the request directly and MUST NOT reply `421` under any
  circumstance

#### Scenario: Existing single-node HTTP test suite keeps passing unchanged

- GIVEN the pre-existing `test_servidor_cola.py` suite, written against a single
  non-clustered node
- WHEN it is run against a node started in single-node mode
- THEN every test MUST pass unchanged, because single-node mode preserves the exact
  pre-change HTTP surface and status codes
