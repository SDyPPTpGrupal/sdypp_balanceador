# Balanceador Queue Integration Specification

## Purpose

Defines how the balanceador consumes the replicated queue cluster: seed-list
configuration from `BA_COLA_URL` and `consola.py`, `derivar()` and `recolectar()` status
handling against the wider status space a cluster introduces (`421`, `503`,
connection failure, `204`), and the additive cluster fields on `salud()` / `/health`.
`ClienteReplica` itself (discovery, redirect-following, backoff) is specified in
`queue-client-failover`; this spec covers how the balanceador wires and reacts to it.

## Requirements

### Requirement: BA_COLA_URL Accepts a Comma-Separated Seed List

`BA_COLA_URL` MUST accept a comma-separated list of one or more node URLs and the
balanceador MUST parse it into a list before constructing its `ClienteReplica`. This is new
behavior: prior to this change, `BA_COLA_URL` held exactly one URL and was passed straight
to `ClienteCola(COLA_URL, COLA_TOKEN)`.

#### Scenario: A three-node BA_COLA_URL is parsed into a seed list

- GIVEN `BA_COLA_URL=http://127.0.0.1:8085,http://127.0.0.1:8086,http://127.0.0.1:8087`
- WHEN the balanceador starts
- THEN it MUST construct `ClienteReplica` with all three URLs as its seed list

#### Scenario: A single-URL BA_COLA_URL still works

- GIVEN `BA_COLA_URL=http://127.0.0.1:8085` (no commas)
- WHEN the balanceador starts
- THEN it MUST construct `ClienteReplica` with a one-element seed list, and the balanceador
  MUST operate correctly against that single node, matching pre-change behavior

### Requirement: consola.py Generates a Seed List, Not a Single URL

`consola.py`'s generation of the balanceador's queue configuration MUST produce a
comma-separated list of the cluster's node URLs rather than a single URL, consistent with
`BA_COLA_URL`'s new accepted format.

#### Scenario: consola.py output matches a running N-node cluster

- GIVEN `consola.py` configuring a balanceador to talk to a 3-node queue cluster
- WHEN it writes the environment configuration consumed at balanceador startup
- THEN the generated `BA_COLA_URL` value MUST list all 3 node URLs, comma-separated

### Requirement: derivar() Never Lets a Redirect Reach the Caller

`derivar()` MUST NOT let a `421` (or any wrong-node signal) surface to the balanceador's own
caller as a generic `502`. Redirect-following is entirely `ClienteReplica`'s responsibility;
by the time `derivar()` observes a result from `ClienteReplica`, it MUST see only a fully
resolved outcome: success, definitive rejection (e.g. `503` queue full), or the exhausted-
budget/leaderless case — never a raw `421`.

#### Scenario: A transient wrong-node hit never becomes a user-visible 502

- GIVEN `ClienteReplica` internally receives one `421` and successfully follows the redirect
- WHEN `derivar()` receives the outcome
- THEN `derivar()` MUST see the eventual success or definitive failure, and MUST NOT
  translate an internal `421` into a `502 "la cola rechazó el pedido"` for the end client

#### Scenario: An exhausted leaderless budget still resolves cleanly

- GIVEN the queue cluster is leaderless past `derivar()`'s wait budget
  (`PRESUPUESTO + GRACIA`)
- WHEN `derivar()`'s call into `ClienteReplica` times out or returns the leaderless-503
  outcome
- THEN `derivar()` MUST surface a clear, definitive failure to its caller (not a bare `502`
  that conflates "queue rejected the pedido" with "cluster currently has no master")

### Requirement: recolectar() Distinguishes 204, 503, 421, and Connection Failure

`recolectar()` MUST treat `204` (no response available yet — a normal long-poll timeout),
`503`/`421` (the cluster or node could not serve the request), and a connection failure as
three distinct outcomes, not collapsed into one `None`. Only the `204` case MUST be treated
as "nothing to do, poll again immediately within the loop's normal cadence." The `503`/`421`/
connection-failure cases MUST cause `recolectar()` to sleep (`time.sleep(ESPERA_REINTENTO)`
or equivalent) before retrying, to avoid a closed retry loop against a struggling or
leaderless cluster.

#### Scenario: A 204 does not trigger backoff sleep

- GIVEN `ClienteReplica.tomar_respuesta()` returns the equivalent of "204, no response yet"
- WHEN `recolectar()` processes that outcome
- THEN it MUST loop again at its normal polling cadence, without an extra backoff sleep

#### Scenario: A leaderless cluster does not spin recolectar() in a closed loop

- GIVEN `ClienteReplica.tomar_respuesta()` returns the equivalent of "503/421, cluster has
  no master right now"
- WHEN `recolectar()` processes that outcome
- THEN it MUST sleep for `ESPERA_REINTENTO` before its next attempt, and observed CPU usage
  over that window MUST stay near zero, not spin in a closed loop

#### Scenario: A connection failure also triggers backoff, distinctly from 204

- GIVEN `ClienteReplica.tomar_respuesta()` raises or returns a connection-failure outcome
- WHEN `recolectar()` processes that outcome
- THEN it MUST sleep before retrying, exactly as it does for the `503`/`421` case, and MUST
  NOT treat the connection failure as equivalent to "204, nothing to do"

### Requirement: salud() Reports Additive Cluster Fields

`salud()` MUST add the following fields without removing or renaming any existing field:
`cola.rol` (role of the node that last answered, expected to normally be `"master"`),
`cola.termino` (the currently known cluster term), `cola.estado` (`"sana"` if a master is
currently answering, `"eligiendo"` if no master is currently known, `"caída"` if no node in
the cluster responds at all), and `cola.instancias` (a list of `{url, instancia, rol,
termino}` for each node in the seed list, best-effort).

#### Scenario: A healthy cluster reports sana with the master's role and term

- GIVEN a 3-node cluster with a responding master at term 4
- WHEN `salud()` is queried
- THEN it MUST report `cola.estado: "sana"`, `cola.rol: "master"`, `cola.termino: 4`

#### Scenario: An electing cluster reports eligiendo

- GIVEN a cluster where no node currently reports being master
- WHEN `salud()` is queried
- THEN it MUST report `cola.estado: "eligiendo"`

#### Scenario: A fully down cluster reports caída

- GIVEN every node in the seed list is unreachable
- WHEN `salud()` is queried
- THEN it MUST report `cola.estado: "caída"`

#### Scenario: Existing salud() consumers are unaffected by the additive fields

- GIVEN an existing consumer of `salud()`'s output that only reads pre-existing fields
- WHEN the new `cola.rol` / `cola.termino` / `cola.estado` / `cola.instancias` fields are
  added
- THEN that consumer's behavior MUST be unaffected, because the addition is purely additive
  to the existing shape

### Requirement: The Balanceador Suite Does Not Regress Below Its Baseline

The balanceador's test suite (`python -m unittest discover -s tests`) MUST continue to pass
at or above its pre-change baseline of 100 tests. Any change to this capability's area MUST
NOT reduce that count without an explicit, stated reason recorded alongside the change.

#### Scenario: Full suite run after wiring ClienteReplica

- GIVEN the balanceador wired to use `ClienteReplica` with a seed list
- WHEN `python -m unittest discover -s tests` runs
- THEN it MUST report at least 100 passing tests, matching or exceeding the pre-change
  baseline
