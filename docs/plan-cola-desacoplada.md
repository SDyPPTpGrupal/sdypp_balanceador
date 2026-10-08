# Desacoplar el sistema de colas: servicio propio con replicación master/slave (Raft simplificado)

> **Propuesta para revisar en equipo** — 2026-09-20, rama `feature/doc-desacople`. Todavía no se
> implementó nada. Reemplaza la propuesta anterior de partición sin estado compartido: se decidió
> en equipo que la prioridad es **no perder pedidos si cae el nodo que los tiene**, y eso exige un
> clúster con una única cola lógica replicada, no N colas independientes.

## Contexto

Hoy la cola ya es un contenedor aparte (`cola/`), pero **no está desacoplada**: vive en el repo del
balanceador, `consola.py` la construye y la levanta, comparte el `balanceador.env`, corre con
`--network host` al lado del balanceador, y todo su estado son `deque`/`dict` en la memoria de un
proceso protegidos por un `threading.Condition`. Hay **exactamente una** instancia posible: si se
cae, se pierde todo lo que tenía en vuelo.

Lo que se busca:

1. Que la cola sea un **servicio propio en su propio repositorio**, desplegable en otra máquina.
2. **Un clúster de mínimo 3 nodos** (impar, para poder desempatar por mayoría) formando **una única
   cola lógica**: un nodo **master** que acepta lecturas y escrituras, y N−1 **slaves** que replican
   el log del master y están listos para ser promovidos si el master cae.
3. **No perder pedidos aceptados**, aunque el master se caiga: la confirmación al que publica llega
   sólo después de que una **mayoría** del clúster ya tiene esa operación en su log.
4. Que la semántica de cola siga siendo **código propio** — nada de RabbitMQ, Kafka, Celery, Redis ni
   una librería de consenso externa. La imagen de la cola sigue sin correr `pip install`.
5. Un **contrato de tareas explícito y versionado**, con el push y el pull definidos.

### La decisión que ordena todo: una cola lógica, replicada por Raft simplificado

**Un único master concentra todas las lecturas y escrituras. Los slaves sólo replican el log y
compiten por ser el próximo master. Nunca se lee ni se escribe contra un slave.**

Esto último merece la aclaración que en la primera versión de este documento se pasó por alto:
`POST /pedidos/tomar` **no es una lectura**, es una mutación (reserva el pedido y lo saca del pool
de disponibles). Si dos workers pudieran "tomar" contra dos slaves distintos, la garantía de
"a-lo-sumo-una-entrega" para pedidos no idempotentes se rompe apenas la replicación tenga el más
mínimo retraso. Por eso **todo el tráfico de pedidos y respuestas va siempre al master**; los
slaves existen únicamente para la elección y la recuperación del estado.

Lo que se gana:

| | Partición (N colas independientes) | Master/slave + Raft |
| :--- | :--- | :--- |
| Pérdida ante caída de un nodo | se pierden los pedidos de esa instancia (1/N) | **cero**, si ya fueron confirmados (mayoría los tiene) |
| Orden | FIFO por instancia, N filas | FIFO única, una sola fila |
| Cota | N × `COLA_COTA_PEDIDOS` | una sola cota, la del clúster |
| Complejidad | reparto simple, sin coordinación | elección de líder, log replicado, fencing por término |
| Latencia de publicación | la de un solo nodo | la de esperar el ack de la mayoría (RTT a 1 slave extra con 3 nodos) |

Lo que se paga, y hay que decirlo en el informe:

- **Latencia de escritura mayor**: cada `POST /pedidos` espera el ack de mayoría antes de devolver
  `202`. Con 3 nodos, eso es esperar a que **al menos 1** de los 2 slaves confirme.
- **Ventana de indisponibilidad durante la elección**: mientras el clúster no tiene master (el
  anterior murió y todavía no se eligió uno nuevo), el clúster no acepta escrituras. Se acota con el
  timeout de elección (con jitter, como en Raft real).
- **Un pedido en vuelo en el momento exacto de la caída del master, que todavía no llegó a mayoría,
  se pierde igual.** Es inevitable con replicación semi-síncrona: la garantía es "si el cliente
  recibió `202`, el pedido sobrevive a la caída de un nodo (el master)", no "cero pérdida bajo
  cualquier circunstancia".

### Las reglas de las que depende la corrección

**Regla 1 — sólo se escribe y se lee contra el master.** `/pedidos`, `/pedidos/tomar`,
`/pedidos/devolver`, `/respuestas`, `/respuestas/tomar` van siempre al master vigente. Un nodo que
no es master devuelve `421 {"error": "no-soy-master", "master": "<url o null>"}` y el cliente
reintenta contra la URL indicada (o descubre uno nuevo si viene `null`, ver más abajo).

**Regla 2 — una entrada del log recién se aplica y se confirma cuando la tiene la mayoría.** El
master no responde `202` a un `POST /pedidos` hasta que `ceil((N+1)/2)` nodos del clúster (contando
al propio master) tienen esa entrada en su log de replicación. Es lo que garantiza que, si el master
muere justo después, el nuevo master —que por construcción del algoritmo de elección tiene el log
más actualizado entre los que votaron la mayoría— ya la tenga.

**Regla 3 — el término (`term`) desempata siempre.** Todo mensaje entre nodos del clúster
(heartbeat, replicación, voto) lleva el término del emisor. Un nodo que ve un término mayor al
propio se retracta inmediatamente a slave, sin importar en qué rol estaba. Es el mecanismo de
fencing que evita el split-brain: un master viejo que vuelve de una partición de red ve términos más
nuevos circulando y deja de aceptar escrituras.

**Regla 4 — nada de nginx ni de un balanceador HTTP adelante del clúster de colas.** El balanceador
y los workers hablan directo con los nodos y siguen al líder ellos mismos; un proxy round-robin
mandaría escrituras a un slave al azar, que las va a rechazar con `421`, o peor, las aceptaría si el
proxy no entiende el protocolo. Esto va escrito en el README.

### Descubrimiento del master — cómo llegan el balanceador y los workers al clúster

Esta sección aplica **igual** al balanceador y a los workers: los dos son clientes del clúster y los
dos resuelven el problema con el mismo mecanismo. Está escrita aparte porque el worker lo escribe
otro integrante y es lo que tiene que implementar.

**La idea que ordena todo: la información de quién es el master ya vive en el clúster.** No hace
falta un tercero que la guarde, porque cualquier nodo la sabe — incluso un slave, que por definición
sabe de quién viene el heartbeat que está recibiendo. En el plan eso sale por `/health` como `rol` y
`masterConocido`.

#### Arranque en frío

El cliente arranca con una **lista estática de las URLs de los N nodos** (la *seed list*): tres
strings en una variable de entorno, nada más. No sabe cuál es el master, y no hace falta que lo sepa.
Le pega a cualquiera y resuelve por una de dos vías, que se complementan:

| Vía | Cómo | Cuándo conviene |
| :--- | :--- | :--- |
| **Preguntar** | `GET /health` a cualquier nodo vivo → `{"rol": "slave", "masterConocido": "http://cola-2:8085"}` | arranque en frío, o cuando el `421` vino con `master: null` |
| **Ir directo y comerse el redirect** | manda la operación a quien sea; si le tocó un slave recibe `421 {"error":"no-soy-master","master":"..."}` y reintenta ahí | régimen normal: en el camino feliz no cuesta ningún round trip extra |

#### Régimen estable: costo cero

Una vez descubierto, el cliente **cachea la URL del master** y la usa para todos los pulls y pushes
siguientes. El descubrimiento ocurre sólo dos veces en la vida del proceso: al arrancar y cuando algo
se rompe. En operación normal no hay ningún overhead: es un `POST` directo al master, igual que hoy
contra la cola única.

#### Cuando cae el master

1. El long-poll abierto contra el master se corta (connection reset/refused, o timeout).
2. El cliente **no sabe** quién es el nuevo master, y probablemente **todavía no haya ninguno**: el
   clúster necesita que expire `RAFT_ELECCION_TIMEOUT_MS` en algún slave y que se resuelva la
   votación.
3. El cliente vuelve a correr el descubrimiento recorriendo su seed list. Durante la ventana de
   elección todos contestan `"rol": "slave"` o `"candidato"` con `masterConocido: null`, así que
   **reintenta con backoff** hasta el `presupuestoMs` del pedido. No hay nada mejor que hacer: nadie
   puede inventar un master que todavía no fue electo, y un proxy adelante tampoco podría.
4. Resuelta la elección, el próximo probe encuentra el nodo que dice `"rol": "master"`, actualiza el
   caché y reabre el long-poll ahí. Se reajustó solo, sin reiniciar ni reconfigurar nada.

Todo el mecanismo de adaptación a la topología son tres cosas: **una lista estática, una URL
cacheada, y reintento con backoff**.

#### El master zombi, y por qué el log lo cubre

Caso a tener claro para el informe: el master viejo revive (o vuelve de una partición) y todavía no
sabe que fue destituido. Un cliente con la URL vieja cacheada le manda un `tomar`.

Está cubierto, y por una razón que vale la pena explicitar: **`tomar` pasa por el log de
replicación** (regla 2), y el zombi no puede comprometer nada porque para eso necesita ack de
mayoría, y la mayoría ya se movió a un término nuevo. Entonces no entrega el pedido: falla. Y en su
primer contacto con cualquier otro nodo ve el término mayor y se retracta a slave (regla 3), con lo
que empieza a contestar `421 no-soy-master` y el cliente se redirige solo.

El detalle fino: la decisión de meter `tomar` en el log —que a primera vista parece burocracia— es
justamente lo que hace **segura** una lectura contra un master obsoleto. Si `tomar` se resolviera
con el estado local sin pasar por el log, el zombi podría entregar un pedido que el clúster nuevo ya
le dio a otro.

#### Alternativas descartadas, y por qué

Esto queda escrito porque es exactamente lo que alguien va a querer "mejorar" después:

- **Los workers entrando por el balanceador (proxy de todo el tráfico).** Descartado. Los `tomar`
  son long-polls: el balanceador tendría que sostener una conexión y un hilo bloqueados por cada
  hilo de pull de cada réplica, además del tráfico de clientes. Y peor: convierte al balanceador en
  el punto único de falla de **todo** el sistema. Replicamos la cola con Raft y acks de mayoría
  justamente para sobrevivir a la caída de un nodo; embudar todo por un único balanceador sin
  replicar haría que la disponibilidad dependa de ese proceso y no del clúster. Además la respuesta
  atravesaría el balanceador dos veces (worker → balanceador → cola → recolector del balanceador →
  cliente).
- **El balanceador como punto único de descubrimiento** (expone "¿quién es el master?" y los workers
  le preguntan a él). Descartado. Lo único que compra es cosmético —el worker configura una URL en
  vez de tres— y lo que cuesta es que el worker dependa de que el balanceador esté vivo **para poder
  redescubrir**, o sea justo en el peor momento: cuando el master se cayó y todos necesitan
  redescubrir a la vez. La seed list no tiene ese problema porque no es un componente, es
  configuración: no se puede "caer" una lista de tres strings.
- **Un registro de nodos en una base de datos.** Descartado por ahora, y con dos motivos distintos.
  El operativo: ni el balanceador ni la cola tienen hoy cliente de base alguno (`requirements.txt`
  es sólo `grpcio` + `protobuf`) y la imagen de la cola no corre `pip install`. El de fondo, que es
  el importante: si en la base se guardara **quién es el master**, esa escritura no estaría protegida
  por el término, con lo cual un master zombi podría reclamar el liderazgo sobrescribiendo la fila y
  nada lo rechazaría — se abre por la ventana el split-brain que cierra la regla 3. Si algún día se
  agrega un registro, va a servir para la **membresía** (qué nodos existen, dato que casi no cambia),
  nunca para el **liderazgo** (que cambia en cada failover y es del clúster). Y el quórum tampoco
  puede leerse de una tabla: pasar de 3 a 5 nodos cambia la mayoría de 2 a 3, y si dos nodos leen la
  membresía en momentos distintos pueden formarse dos mayorías disjuntas, o sea dos masters.

---

## Etapas

Las etapas 1 y 2 se hacen **en este repo** y dejan todo funcionando con un clúster local
(mínimo 3 nodos) sin separar el repositorio todavía. Recién la 3 separa.

### Etapa 1 — La cola: rol Raft-lite, log de replicación y contrato versionado

Archivos: `cola/servidor.py`, `cola/colas.py`, `cola/raft.py` (nuevo), `cola/README.md` (nuevo
`cola/CONTRATO.md`).

`colas.py` **no cambia su lógica de negocio** (encolar, tomar, devolver, expirar). Internamente ya
tiene, en `Sistema`, dos colas de primer nivel con dueños distintos (`colas.py:359-373`):

- **`ColaPedidos`**: los pedidos que llegaron por `POST /pedidos`. Adentro no es una sola
  estructura, son tres (`colas.py:94-107`): `_esperando` (deque, nadie los tomó todavía),
  `_en_vuelo` (dict por id, reservados por un worker y sin contestar) y `_a_fallar` (deque interno
  que el recuperador drena). Es la que alimenta `POST /pedidos/tomar`.
- **`ColaRespuestas`**: las respuestas que llegaron por `POST /respuestas`, indexadas por
  `destinatario` porque el balanceador las consume por dueño, no en orden global
  (`colas.py:279-297`). Es la que alimenta `POST /respuestas/tomar`.

**No hay una tercera cola de TTL separada, y no hace falta agregarla.** La expiración no está
indexada aparte: cada `Pedido` ya lleva su propio `vence_en`/`reservado_hasta`
(`colas.py:53-63`), y cada respuesta guarda su instante de publicación junto al contenido
(`colas.py:313`). El vencimiento se calcula **perezosamente**, barriendo `_esperando`/`_en_vuelo` en
`ColaPedidos.recuperar()` (`colas.py:194-246`) y los deques de `_por_destinatario` en
`ColaRespuestas.purgar()` (`colas.py:333-345`). Documentar una cola de TTL aparte sería describir
algo que no existe, y replicarla sería trabajo de más sin ninguna ganancia.

Lo que cambia con la replicación es que cada mutación sobre estas dos colas deja de aplicarse
directo: pasa a ser una **entrada de log** append-only con `{indice, termino, operacion, payload}`
(`operacion` es una de `encolar`, `tomar`, `devolver`, `responder`, `retirar-respuesta`, `expirar`),
y sólo se aplica al estado de `colas.py` cuando esa entrada está comprometida (regla 2). Es el mismo
patrón que una base de datos con WAL: primero se persiste la intención en el log, después se aplica.
La expiración (`recuperar()`/`purgar()`) también pasa por el log: es el master el que detecta el
vencimiento con su propio reloj monótono y **replica la decisión**, para que los slaves no la
recalculen cada uno con el suyo y terminen en estados distintos.

`cola/raft.py` — máquina de estados por nodo (`master` / `slave` / `candidato`):

- **Estado persistente por nodo**: `termino_actual`, `voto_para` (en qué candidato votó este
  término), el log de entradas.
- **Heartbeats**: el master manda `POST /raft/appendEntries` vacío (sin entradas nuevas) a cada
  slave cada `RAFT_HEARTBEAT_MS`. Si un slave no recibe nada durante `RAFT_ELECCION_TIMEOUT_MS`
  (con jitter aleatorio por nodo, para que no todos se postulen al mismo tiempo), dispara una
  elección.
- **Elección**: el nodo incrementa su término, se vota a sí mismo, pide voto a los demás con
  `POST /raft/requestVote {termino, candidato, ultimoIndiceLog, ultimoTerminoLog}`. Un nodo vota
  "sí" sólo si no votó ya en ese término y el log del candidato está **al menos tan actualizado**
  como el propio (mismo criterio que Raft: se compara primero el término de la última entrada, y a
  igualdad, el índice). Con mayoría de votos, el candidato pasa a master y empieza a mandar
  heartbeats con su nuevo término.
- **Replicación**: `POST /raft/appendEntries {termino, entradas[], indiceCommit}` — el slave agrega
  las entradas nuevas a su log en el mismo orden y contesta con el índice más alto que aplicó. El
  master calcula la mayoría y avanza `indiceCommit`; recién ahí aplica el efecto localmente y
  desbloquea el `202` que estaba esperando esa entrada.

En `servidor.py`:

- `COLA_INSTANCIA` (default: `f"{NOMBRE}-{PUERTO}@{CASA}"`). Sale en `/health`, en `/estado`, en
  cada pedido y en la bitácora.
- **Versión de contrato declarada y verificada en caliente**, igual que en la propuesta anterior:
  `GET /health` devuelve `"contrato": "1.0"`.
- `GET /health` agrega `"rol": "master"|"slave"`, `"termino"`, `"masterConocido": "<url o null>"`.
- Rutas internas de clúster (no forman parte del contrato público de tareas, son protocolo entre
  nodos de la cola): `POST /raft/appendEntries`, `POST /raft/requestVote`, `GET /raft/estado`
  (debug: rol, término, índice de log, índice de commit).
- **Partir el token en dos**, igual que antes: `COLA_TOKEN_PUBLICADOR` (balanceador) y
  `COLA_TOKEN_CONSUMIDOR` (workers). Un tercer secreto, `COLA_TOKEN_CLUSTER`, protege las rutas
  `/raft/*` para que un nodo ajeno no pueda postularse candidato ni inyectar entradas de log.

### Etapa 2 — El balanceador y los workers como clientes que siguen al líder

Archivos: `app/clientecola.py` (modificado), **`app/clientereplica.py` (nuevo, reemplaza al
`app/anillo.py` de la propuesta anterior)**, `app/balanceador.py`.

`BA_COLA_URL` sigue aceptando una **lista separada por comas** — pero ahora son los N nodos del
mismo clúster, no N colas independientes.

#### `app/clientereplica.py` — `ClienteReplica`

Misma superficie que `ClienteCola` para que `derivar()` cambie lo mínimo:

```python
class ClienteReplica:
    def __init__(self, urls, token="", timeout=5.0, conexiones=8)
    def publicar_pedido(self, pedido)   # va contra el master conocido; si no lo conoce, lo busca
    def tomar_respuesta(self, ...)      # idem
    def estado(self)                    # del master
    def master_conocido(self)           # último master exitoso, o None
```

**Descubrir al master**: el cliente guarda el último master que le funcionó. Si la request le da
`421 no-soy-master` con un `master` en el cuerpo, actualiza y reintenta ahí mismo (una sola vez). Si
no tiene ningún master conocido (arranque en frío, o el `421` vino con `master: null` porque el
clúster está en elección), hace un *probe* secuencial de los N nodos preguntando `/health` hasta
encontrar uno que diga `"rol": "master"`. Si ninguno lo es, el clúster está sin líder: se reintenta
con backoff hasta el `presupuestoMs` del pedido.

**El failover, igual que antes distingue si el POST llegó a escribirse o no** (mismo criterio de
`ErrorCola.enviado` que ya existe en `app/clientecola.py:87-101`), pero ahora la consecuencia es
distinta: si falló por conexión, se reintenta el *probe* de master; si falló por `421 no-soy-master`,
se sigue directo al master indicado sin duplicar el POST.

Si el clúster entero no responde o está en elección más allá del presupuesto: `503
{"error": "el sistema de colas no responde"}`.

#### Recolección

`recolectar()` (`app/balanceador.py:346`) sigue usando un único `ClienteReplica` compartido (no uno
por instancia, como en la propuesta de partición): sólo hay un master del que recolectar en cada
momento, y el cliente ya sabe encontrarlo.

> **Mismo bug a evitar que en la propuesta anterior.** Si el nodo contactado devuelve `503` o `421`
> (no un corte de conexión), la respuesta a `tomar_respuesta` no puede tratarse como "no había nada"
> (`204`): hay que distinguirlo y aplicar `time.sleep(ESPERA_REINTENTO)` antes de reintentar, o se
> genera un bucle cerrado contra un clúster que está en elección.

#### `/health` del balanceador

`salud()` (`app/balanceador.py:550-604`) agrega:

- `cola.rol`: rol del nodo que contestó la última vez (debería ser siempre `master`).
- `cola.termino`: término actual conocido del clúster.
- `cola.estado`: `"sana"` si hay master respondiendo, `"eligiendo"` si no hay master vigente,
  `"caída"` si ningún nodo del clúster responde.
- `cola.instancias`: lista con `{url, instancia, rol, termino}` de cada nodo, para poder ver en la
  consola si hay algún slave desincronizado.

### Etapa 3 — Separar el repositorio

```bash
git subtree split -P cola -b cola-sola    # conserva la historia de esos archivos
```

**Repo nuevo `sdypp_cola`:**

```
README.md          ampliado con el protocolo Raft-lite y las reglas de arriba
CONTRATO.md         la fuente de verdad del contrato v1 (ver abajo)
Dockerfile          el de hoy, sin cambios
colas.py            el de hoy, ahora aplicado sólo sobre entradas comprometidas del log
raft.py             máquina de estados: elección, heartbeats, replicación
servidor.py         el de hoy + rol, /raft/*, tokens partidos en tres
cliente.py          ClienteReplica, publicado por el repo de la cola: es parte del contrato
consola.py          genera cola.env y levanta un clúster de N≥3 nodos (impar) en puertos consecutivos
tests/
  test_colas.py            movido tal cual
  test_raft.py             nuevo: elección con mayoría, fencing por término, no hay doble master
  test_servidor_cola.py    movido; importa `cliente.py` de este repo
```

**El cliente se publica desde el repo de la cola y el balanceador lo vendorea**, igual que en la
propuesta anterior: `cliente.py` se copia a `app/clientecola.py`/`app/clientereplica.py` con una
cabecera `# copiado de sdypp_cola@v1.0 — no editar acá`, y un test propio
(`tests/test_cliente_vendoreado.py`) verifica que la versión declarada coincide con la que anuncia
`/health`.

**Qué queda en `sdypp_balanceador`:** se borra `cola/`, se mueven `tests/test_colas.py` y
`tests/test_servidor_cola.py`, y se agrega **`tests/test_cluster_cola.py`**: un clúster de mentira
(nodos `http.server` mínimos con las rutas de datos y de `/raft/*`) contra el que se prueban el
seguimiento del líder, el failover ante caída del master, y que el balanceador nunca escribe contra
un slave.

### Etapa 4 — Despliegue y consola de la cola

`consola.py` del repo de la cola: pregunta cuántos nodos (mínimo 3, valida que sea impar si son más),
genera `cola.env` con los tres tokens (`secrets.token_urlsafe(32)`) y levanta N contenedores
`sdypp-cola-1..N` en puertos consecutivos. Red **bridge con `-p`**, no `--network host`. El menú 1
pasa a verificar el clúster y mostrar, por nodo, rol y término — y avisa si hay más de un nodo que
se cree master (sería un bug de fencing, no debería poder pasar).

El `HEALTHCHECK` del contenedor apunta a `GET /health/vivo` (200 mientras el proceso atienda),
separado de `GET /health` (el que miran la consola y el balanceador).

---

## El contrato de tareas — `CONTRATO.md` v1

La forma de los mensajes de datos (`/pedidos`, `/pedidos/tomar`, `/respuestas`,
`/respuestas/tomar`) **no cambia respecto de la propuesta anterior** salvo un detalle: ya no viaja
el campo `cola` en el pedido (ese campo tenía sentido cuando había N colas independientes; con una
única cola lógica, contestar "a la cola equivocada" ya no es un caso posible: siempre es al master).
Lo que sí es nuevo es la respuesta cuando el nodo contactado no es el master.

### Cualquier ruta de datos, contra un nodo que no es master

```jsonc
// ← 421
{"error": "no-soy-master", "master": "http://cola-2:8085"}   // o "master": null si está en elección
```

El publicador/consumidor reintenta contra `master` sin duplicar el POST original (regla 1).

### Push del pedido — `POST /pedidos` (lo manda el balanceador, contra el master)

```jsonc
{"id": "4b7baf0d…",            // opcional, lo genera la cola si falta
 "operacion": "POST /personas", // obligatorio, opaco para la cola
 "parametros": {"nombre": "Ada", "legajo": 1234},
 "idempotente": false,          // default false: ante la duda no se reintenta
 "destinatario": "balanceador@casa-tomas",   // obligatorio
 "cliente": "100.118.61.111",
 "presupuestoMs": 5000}
// ← 202 {"id", "encolado": true} — sólo después de comprometer la entrada en mayoría del clúster
// ← 503 {"error": "cola llena", "esperando": 100, "cota": 100}
// ← 421 no-soy-master (ver arriba)
```

### Pull del pedido — `POST /pedidos/tomar` (lo consume el worker, contra el master)

Entrada `{consumidor, espera}` — long-poll. `consumidor` sigue siendo el `host:puerto` gRPC de la
réplica del worker.

```jsonc
// ← 200
{"id": "4b7baf0d…", "operacion": "POST /personas",
 "parametros": {"nombre": "Ada", "legajo": 1234}, "idempotente": false,
 "cliente": "100.118.61.111",
 "quedaMs": 4870,
 "intento": 1}
// ← 204 sin cuerpo. No había trabajo.
// ← 421 no-soy-master
```

**Ningún instante absoluto viaja en ninguna dirección**, sólo `quedaMs`, calculado con el reloj del
master al momento de servir.

### Push de la respuesta — `POST /respuestas` (lo manda el worker, contra el master)

`{id, estado, contenido, atendidoPor, app}`. `destinatario` no se manda: lo pone la cola desde el
pedido original.

`202 {"resultado":"entregada"}` (comprometido en mayoría antes de contestar) ·
`409 {"resultado":"desconocido"}` (ya la contestó otro — no reintentar) ·
`409 {"resultado":"destinatario-saturado"}` · `421 no-soy-master`.

### Pull de la respuesta — `POST /respuestas/tomar` (lo consume el balanceador, contra el master)

Entrada `{destinatario, espera}`, long-poll.
Salida `{id, operacion, estado, contenido, atendidoPor, app, intentos: [...], esperaMs}`.

### Garantías

1. **Al-menos-una-vez para `idempotente: true`.** El worker debe tolerar re-ejecución; `intento`
   dice cuántas veces se entregó antes.
2. **A-lo-sumo-una-entrega para `idempotente: false`.** Se sostiene porque `tomar` sólo se resuelve
   contra el master (regla 1): no hay forma de que dos réplicas del clúster entreguen el mismo
   pedido a la vez.
3. **Un pedido confirmado con `202` sobrevive a la caída del master.** Se sostiene porque el `202`
   sólo se emite después de comprometer la entrada en mayoría (regla 2), y el próximo master electo
   forzosamente tiene esa entrada (invariante de la elección Raft: gana el candidato con el log más
   actualizado entre los votantes de la mayoría).
4. **Nunca hay dos masters activos al mismo tiempo aceptando escrituras.** Se sostiene por el
   fencing de término (regla 3): cualquier nodo que vea un término mayor se retracta.
5. **Exactamente una respuesta entregada por pedido.** Gana la primera; el resto, `409 desconocido`.
6. **FIFO única.** A diferencia de la propuesta de partición, acá hay una sola fila real, porque hay
   una sola cola lógica.
7. **Terminación.** Todo pedido aceptado termina en una respuesta antes de
   `presupuestoMs + COLA_INTERVALO_RECUPERADOR`, salvo que el clúster entero quede sin master más
   tiempo que eso — ahí lo cierra el timeout del balanceador (`PRESUPUESTO + GRACIA`) con un 504.
8. **Versionado.** Igual que antes: agregar un campo opcional es v1; quitar, renombrar, o cambiar el
   significado de un código es v2, y v1 y v2 conviven durante el despliegue.

### Lo que cambia para quien escribe el worker

**Está todo en `docs/contrato-worker.md`**, que es el documento que se le entrega al equipo que
desarrolla el worker: contrato completo, descubrimiento del master, garantías, trampas, pseudocódigo
y checklist de aceptación. Es autocontenido a propósito — ese equipo no necesita leer este plan.

**Ese documento es la fuente de verdad del lado del worker. No dupliques nada de eso acá**, o en dos
semanas los dos textos dicen cosas distintas y nadie sabe cuál vale.

Resumen de una línea para quien lee este plan: el worker habla **directo** con el clúster (no por el
balanceador), arranca con una seed list, descubre y cachea el master, sigue el `421 no-soy-master`
cuando cambia, y **no implementa nada de Raft**.

---

## Verificación

**Clúster de 3, a mano:**

```bash
for p in 8085 8086 8087; do
  COLA_PUERTO=$p COLA_INSTANCIA=cola-$p COLA_PARES=8085,8086,8087 python3 cola/servidor.py &
done
BA_COLA_URL=http://127.0.0.1:8085,http://127.0.0.1:8086,http://127.0.0.1:8087 \
  python3 app/balanceador.py
./.venv/bin/python app/verificador.py
```

Qué mirar: exactamente un nodo dice `"rol": "master"` en `/health`; los otros dos, `"slave"`; matar
al master dispara una elección visible en la bitácora en menos de `RAFT_ELECCION_TIMEOUT_MS`, y el
nuevo master retoma los pedidos ya comprometidos.

**Tests nuevos:**

- `tests/test_raft.py` (repo de la cola): elección con mayoría de 3 y de 5 nodos; un candidato con
  log desactualizado no puede ganar; fencing — un nodo con término viejo se retracta al ver uno
  nuevo; no puede haber dos masters simultáneos incluso con partición de red simulada.
- `tests/test_cluster_cola.py` (repo del balanceador, clúster de mentira): el `ClienteReplica`
  encuentra al master en frío; sigue el `421 no-soy-master` sin duplicar el POST; reintenta con
  backoff si el clúster está sin líder; nunca manda `/pedidos/tomar` a un nodo que contestó
  `"rol": "slave"`.
- `tests/test_servidor_cola.py`: se agrega que un `202` sólo sale después de que el log tiene
  confirmación de mayoría (se puede simular con slaves lentos y verificar que el `202` espera).
- `tests/test_colas.py`: sin cambios, la lógica de cola no se tocó.

**Caos, para el informe:**

| | Qué se hace | Qué tiene que pasar |
| :--- | :--- | :--- |
| 0 | matar al master con un worker con long-poll abierto | el worker ve el corte, recorre su seed list con backoff y reengancha solo contra el master nuevo — sin reiniciarlo ni reconfigurarlo |
| 1 | matar al master con pedidos ya confirmados (`202`) en vuelo | ninguno se pierde: el nuevo master los tiene y los entrega |
| 2 | matar al master con un `POST /pedidos` en curso que **todavía no** llegó a mayoría | ese pedido puntual se pierde; el cliente ve el error de conexión y puede reintentar (no es idempotente automáticamente) |
| 3 | partición de red que aísla al master viejo con un slave (minoría) | el viejo master deja de poder comprometer nada (no tiene mayoría) y dentro de `RAFT_ELECCION_TIMEOUT_MS` el lado con mayoría elige uno nuevo; al sanar la partición, el viejo master ve el término nuevo y se retracta a slave sin haber aceptado escrituras huérfanas |
| 4 | **master zombi**: revivir al master viejo mientras un worker todavía tiene su URL cacheada | el zombi no puede comprometer el `tomar` (no junta mayoría, el término quedó viejo), así que no entrega nada; se retracta a slave al primer contacto y empieza a contestar `421 no-soy-master`, con lo que el worker se redirige solo |
| 5 | levantar de nuevo un slave caído | se resincroniza el log solo (catch-up de las entradas que le faltan) |
| 6 | apagar todo el clúster | 503 "el sistema de colas no responde"; CPU del balanceador **y de los workers** cerca de cero (backoff, no bucle cerrado) |
