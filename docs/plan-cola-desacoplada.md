# Desacoplar el sistema de colas: servicio propio, N instancias activas

> **Propuesta para revisar en equipo** — 2026-09-18, rama `asincronico`. Todavía no se implementó nada.
> Lo que más necesita acuerdo antes de arrancar: la **partición sin estado compartido** (abajo), y el
> punto **"Lo que cambia para quien escribe el worker"**, que toca el código de otro integrante.

## Contexto

Hoy la cola ya es un contenedor aparte (`cola/`), pero **no está desacoplada**: vive en el repo del
balanceador, `consola.py` la construye y la levanta, comparte el `balanceador.env`, corre con
`--network host` al lado del balanceador, y todo su estado son `deque`/`dict` en la memoria de un
proceso protegidos por un `threading.Condition`. Hay **exactamente una** instancia posible.

Lo que se busca:

1. Que la cola sea un **servicio propio en su propio repositorio**, desplegable en otra máquina.
2. **N instancias activas** atendiendo al mismo tiempo.
3. Que la semántica de cola siga siendo **código propio** — nada de RabbitMQ, Kafka, Celery ni Redis.
   La imagen de la cola sigue sin correr `pip install`.
4. Un **contrato de tareas explícito y versionado**, con el push y el pull definidos.

### La decisión que ordena todo: partición, no estado compartido

**N instancias independientes, cada una con su propia cola en memoria. Un pedido vive en exactamente
una instancia. El reparto lo hacen los clientes.**

Lo que se gana, y es mucho:

| | Con estado compartido (Redis) | Con partición |
| :--- | :--- | :--- |
| Atomicidad del `tomar` | scripts Lua, el problema difícil | gratis: nadie más ve ese pedido |
| Reserva y recuperador | hay que coordinar N recuperadores | queda **tal cual está hoy**, local |
| Relojes | hay que migrar todo a `TIME` de Redis | `time.monotonic()` sigue sirviendo |
| Dependencias | `redis`, y un SPOF nuevo en el camino crítico | **cero**, la imagen sigue sin `pip install` |
| `colas.py` (464 líneas ya probadas) | se reescribe entero | **no se toca** |

Lo que se paga, y hay que decirlo en el informe:

- **FIFO por instancia, no global.** Ya era "aproximado": los reasignados vuelven al frente y los
  consumidores compiten. Con N instancias hay N filas.
- **Si cae una instancia se pierden sus pedidos en vuelo.** El cliente ve el 504 que `derivar()` ya
  produce a los `PRESUPUESTO + GRACIA` segundos. Hoy la cola tampoco persiste: reiniciar el
  contenedor ya tiraba todo. La diferencia es que ahora se pierde 1/N en vez de todo.
- **La cota deja de ser global**: son N × `COLA_COTA_PEDIDOS`.

### Las dos reglas de las que depende la corrección

**Regla 1 — la respuesta vuelve a la instancia que dio el pedido.** El pedido vive ahí; una respuesta
a otra instancia recibe `409 desconocido` y el pedido se pierde hasta que venza. Se hace explícita
agregando el campo `cola` al pedido que ve el worker, y sale natural del diseño de pull recomendado
(un hilo por instancia).

**Regla 2 — nada de nginx ni de un balanceador HTTP adelante.** Es tentador y rompe el sistema: el
recolector del balanceador tiene que drenar la cola de respuestas de **cada** instancia; detrás de un
proxy round-robin drenaría una al azar por poll y las respuestas de las otras se quedarían hasta el
TTL. El anillo va **explícito en los clientes**. Esto va escrito en el README, porque es exactamente
lo que alguien va a "arreglar" después.

---

## Etapas

Las etapas 1 y 2 se hacen **en este repo** y dejan todo funcionando con N=1 (sin regresión). Recién
la 3 separa. Así ningún commit intermedio queda roto.

### Etapa 1 — La cola: identidad y contrato versionado

Archivos: `cola/servidor.py`, `cola/colas.py`, `cola/README.md` (nuevo `cola/CONTRATO.md`).

`colas.py` **no cambia su lógica**. Un solo agregado en `Pedido.como_json()` (`cola/colas.py:69`):

```python
"cola": INSTANCIA,   # a esta instancia hay que contestarle. Campo aditivo: sigue siendo v1
```

En `servidor.py`:

- `COLA_INSTANCIA` nueva (default: `f"{NOMBRE}-{PUERTO}@{CASA}"`). Sale en `/health`, en `/estado`,
  en cada pedido y en la bitácora.
- **Prefijo `/v1/` en las cinco rutas de negocio**, con las rutas sin prefijo mantenidas como alias
  (el diccionario `rutas` de `cola/servidor.py:336` acepta las dos formas). La versión en la URL y no
  en un header: se ve en la bitácora, en el `curl` de la demo y en el log de acceso sin inspeccionar
  nada, y el worker lo escribe otro integrante posiblemente en otro lenguaje.
- **Partir el token en dos**: `COLA_TOKEN_PUBLICADOR` (balanceador: `/pedidos`, `/respuestas/tomar`,
  `/estado`) y `COLA_TOKEN_CONSUMIDOR` (workers: `/pedidos/tomar`, `/pedidos/devolver`,
  `/respuestas`). `COLA_TOKEN` se sigue aceptando como comodín para no romper lo ya escrito. Ahora
  que el servicio vive en otra máquina, publicar y consumir dejaron de ser el mismo permiso: hoy un
  worker comprometido puede inyectar pedidos y robar respuestas de cualquier destinatario. Ya está
  anotado como pendiente en `cola/README.md`.
- `GET /health` agrega `"instancia"` y `"contrato": ["1"]`.

### Etapa 2 — El balanceador como cliente de un anillo

Archivos: `app/clientecola.py` (modificado), **`app/anillo.py` (nuevo)**, `app/balanceador.py`.

`ClienteCola` ya es **por host** (`self.host`, `self.puerto`, su propio pool en `app/clientecola.py:36`),
así que se reusa entero: el anillo es una lista de `ClienteCola`, uno por instancia.

**`BA_COLA_URL` pasa a aceptar una lista separada por comas.** Con un solo valor se comporta
exactamente como hoy — compatible hacia atrás y así se prueba la etapa 2 sin desplegar nada nuevo.

#### `app/anillo.py` — `ClienteAnillo`

Misma superficie que `ClienteCola` para que `derivar()` cambie lo mínimo:

```python
class ClienteAnillo:
    def __init__(self, urls, token="", timeout=5.0, conexiones=8)
    def publicar_pedido(self, pedido)              # elige instancia y hace failover
    def clientes(self)                             # para armar un recolector por instancia
    def estado(self)                               # agregado de las N
    def instancias_sanas(self)                     # (sanas, totales)
```

**Publicación — round-robin sobre las instancias vivas**, con un contador bajo lock. No "la menos
cargada": eso costaría un `/estado` por publicación. Las instancias son homogéneas y los workers
tiran de todas, así que el reparto se equilibra solo.

**El failover, que es el punto delicado.** Hoy `_pedir` nunca reintenta sobre una conexión recién
abierta porque reintentar a ciegas duplicaría un `POST /personas` (`app/clientecola.py:87-101`). Con
N instancias hay que distinguir dos fallos que hoy son el mismo `ErrorCola`:

| Cuándo falló | ¿Llegó a la cola? | Qué hace el anillo |
| :--- | :--- | :--- |
| Al conectar o al escribir (`conexion.request`) | **No**, seguro | reintenta en la siguiente instancia |
| Leyendo la respuesta (`getresponse`) | **No se sabe** | sólo reintenta si `idempotente` |
| `503 cola llena` | No, la cola lo dijo | reintenta en la siguiente instancia |

Se implementa agregando un atributo a la excepción en `app/clientecola.py`:
`ErrorCola.enviado = False` cuando el fallo ocurrió antes de `getresponse()`, `True` si después.
Es la misma distinción que el código ya hace para decidir el reintento; sólo hay que exponerla.

Si todas las instancias rebotan: `503 {"error": "cola llena"}` con los números sumados si todas
estaban llenas, `503 {"error": "el sistema de colas no responde"}` si ninguna atendió.

**Instancia caída**: histéresis con `BA_FALLOS_PARA_SACAR` / `BA_EXITOS_PARA_VOLVER`, el mismo patrón
que ya usa `Backend._registrar()` (`app/balanceador.py:168-225`) — se reusa el criterio, no hace falta
inventar otro. Una instancia marcada caída no recibe publicaciones, pero su recolector sigue
reintentando con backoff, así que vuelve sola.

#### Recolección — un grupo de recolectores por instancia

`recolectar()` (`app/balanceador.py:346`) cambia en una línea: recibe el `ClienteCola` de **su**
instancia en vez de usar el global. El despacho por `id` contra `ESPERAS` no cambia nada: ya funciona
venga la respuesta de donde venga. `main()` arranca `BA_RECOLECTORES` hilos **por instancia**
(default baja de 4 a 2, así 3 instancias dan 6 hilos y no 12); se documenta que la semántica pasó a
ser "por instancia".

> **Bug que esta migración destapa si no se toca.** Si una instancia contesta un 503 (no un corte de
> conexión), `tomar_respuesta` devuelve `None` y `recolectar` hace `continue` sin dormir:
> **bucle cerrado contra un servicio que no está bien**. Hoy no se ve porque la cola sólo devuelve 503
> en `/pedidos`. `tomar_respuesta` tiene que distinguir "204, no había nada" de "503/5xx" y tratar el
> segundo como el `ErrorCola` que ya maneja, con `time.sleep(ESPERA_REINTENTO)`.

#### `/health` del balanceador

`salud()` (`app/balanceador.py:550-604`) agrega el desglose sin romper lo que el verificador ya lee:

- `encolados`, `cota`, `enVuelo`, `reasignados`: **suma** de las instancias que contestaron.
- `cola.estado`: `"sana"` si contestan todas, `"degradada"` si algunas, `"caída"` si ninguna.
- `cola.instancias`: lista con `{url, instancia, estado, encolados, enVuelo}`.
- El 503 sigue saliendo sólo si **ninguna** instancia responde, o si no hay réplicas consumiendo.

`backends_json()` (`app/balanceador.py:448-477`) cruza el pool con `consumidores`, que ahora viene de
N instancias: **suma** `enVuelo` y `atendidos` por destino, y toma el **mínimo** de
`ultimoPedidoHaceMs` (la evidencia más reciente de que esa réplica está consumiendo).

### Etapa 3 — Separar el repositorio

```bash
git subtree split -P cola -b cola-sola    # conserva la historia de esos archivos
```

**Repo nuevo `sdypp_cola`:**

```
README.md          el de cola/README.md, ampliado con el anillo y las dos reglas
CONTRATO.md        la fuente de verdad del contrato v1 (ver abajo)
Dockerfile         el de hoy, sin cambios
colas.py           el de hoy + el campo `cola`
servidor.py        el de hoy + instancia, /v1/, tokens partidos
cliente.py         ClienteCola, publicado por el repo de la cola: es parte del contrato
consola.py         genera cola.env y levanta N contenedores en puertos consecutivos
tests/
  test_colas.py            movido tal cual
  test_servidor_cola.py    movido; importa `cliente.py` de este repo
  test_anillo.py           N instancias de verdad: reparto, failover, respuesta a la instancia correcta
```

**El cliente se publica desde el repo de la cola y el balanceador lo vendorea.** El test de contrato
cruzado (`tests/test_servidor_cola.py`) hoy vale precisamente porque habla el servidor real con el
cliente real; si el cliente se queda del lado del balanceador, ese test muere con la separación. Al
revés, sobrevive entero. El balanceador copia `cliente.py` a `app/clientecola.py` con una cabecera
`# copiado de sdypp_cola@v1.0 — no editar acá` y un `CONTRATO = "1.0"`, y un test propio
(`tests/test_cliente_vendoreado.py`) verifica que la versión declarada coincide con la que anuncia
`/health`. Es barato y agarra la deriva.

**Qué queda en `sdypp_balanceador`:** se borra `cola/`, se mueven `tests/test_colas.py` y
`tests/test_servidor_cola.py`, y se agrega **`tests/test_anillo.py`**: instancias de mentira
(un `http.server` mínimo con las cinco rutas) contra las que se prueban el reparto, el failover, la
recolección en abanico y el 503 sin bucle cerrado. Así el repo del balanceador **corre sus tests sin
el repo de la cola** — que es la prueba de que quedaron desacoplados.

**`consola.py`** (`consola.py:92-121`, `:303-352`): se le sacan `IMAGEN_COLA`, `CONTENEDOR_COLA`,
`construir` de la cola, `orden_docker_cola`, `levantar_cola` y todas las `COLA_*` de `DEFAULTS`. Queda
`BA_COLA_URL` (lista) y `BA_COLA_TOKEN`; el token deja de generarse acá (lo genera quien despliega la
cola) y la consola sólo avisa si está vacío. El menú 6 "Contenedores" pierde la cola; el menú 1 pasa a
verificar el anillo y mostrar instancia por instancia.

### Etapa 4 — Despliegue y consola de la cola

`consola.py` del repo de la cola: pregunta cuántas instancias, genera `cola.env` con los tokens
(`secrets.token_urlsafe(32)`, igual que hoy en `consola.py:645`) y levanta N contenedores
`sdypp-cola-1..N` en puertos consecutivos desde `COLA_PUERTO`. Red **bridge con `-p`**, no
`--network host`: en una máquina dedicada no hace falta el host y `-p` da los N puertos limpios.
Alcanzables por la IP de Tailscale de esa máquina.

El `HEALTHCHECK` del contenedor pasa a `GET /health/vivo` (200 mientras el proceso atienda), separado
de `GET /health` (el que miran la consola y el balanceador). Con una sola ruta, cualquier lógica
futura de readiness haría que Docker reinicie instancias sanas.

---

## El contrato de tareas — `CONTRATO.md` v1

Lo que hoy está en `cola/README.md:100-250` se promueve a documento de contrato con versionado y
garantías explícitas. La forma de los mensajes **no cambia**; lo único nuevo es el campo `cola`.

### Push del pedido — `POST /v1/pedidos` (lo manda el balanceador)

```jsonc
{"id": "4b7baf0d…",            // opcional, lo genera la cola si falta
 "operacion": "POST /personas", // obligatorio, opaco para la cola
 "parametros": {"nombre": "Ada", "legajo": 1234},
 "idempotente": false,          // default false: ante la duda no se reintenta
 "destinatario": "balanceador@casa-tomas",   // obligatorio
 "cliente": "100.118.61.111",
 "presupuestoMs": 5000}
// ← 202 {"id", "encolado": true, "cola": "cola-8085@casa-tomas"}
// ← 503 {"error": "cola llena", "esperando": 100, "cota": 100}
```

### Pull del pedido — `POST /v1/pedidos/tomar` (lo consume el worker)

Entrada `{consumidor, espera}` — long-poll. `consumidor` **tiene que ser el `host:puerto` gRPC de la
réplica**: es lo que permite cruzar el registro del balanceador con quién consume, sin traducir nada.

```jsonc
// ← 200
{"id": "4b7baf0d…", "operacion": "POST /personas",
 "parametros": {"nombre": "Ada", "legajo": 1234}, "idempotente": false,
 "cliente": "100.118.61.111",
 "quedaMs": 4870,                      // usalo como timeout del RPC
 "intento": 1,                         // ≥2 = a este pedido ya lo abandonó otra réplica
 "cola": "cola-8085@casa-tomas"}       // ← NUEVO: a esta instancia le contestás
// ← 204 sin cuerpo (Content-Length: 0). No había trabajo.
```

**Ningún instante absoluto viaja en ninguna dirección**, sólo `quedaMs`, calculado con el reloj de la
instancia al momento de servir. Los relojes de cuatro casas no están sincronizados.

### Push de la respuesta — `POST /v1/respuestas` (lo manda el worker)

`{id, estado, contenido, atendidoPor, app}` → **a la instancia de la que vino el pedido**.
`destinatario` no se manda: lo pone la cola desde el pedido original, porque el worker no tiene por
qué saberlo y podría apuntar a otro balanceador.

`202 {"resultado":"entregada"}` · `409 {"resultado":"desconocido"}` (ya lo contestó otro, o le
contestaste a la instancia equivocada — **no reintentar**) · `409 {"resultado":"destinatario-saturado"}`.

### Pull de la respuesta — `POST /v1/respuestas/tomar` (lo consume el balanceador)

Entrada `{destinatario, espera}`, long-poll, **contra cada instancia**.
Salida `{id, operacion, estado, contenido, atendidoPor, app, intentos: [...], esperaMs, cola}`.

### Garantías

1. **Al-menos-una-vez para `idempotente: true`.** El worker debe tolerar re-ejecución; `intento` dice
   cuántas veces se entregó antes.
2. **A-lo-sumo-una-entrega para `idempotente: false`.** Si vence la reserva se falla con
   `DEADLINE_EXCEEDED`, no se reentrega. Honestidad necesaria: la garantía es sobre la **entrega**, no
   sobre la ejecución — si la réplica alcanzó a escribir y murió antes de contestar, el cliente ve un
   504 sobre un alta que quizá ocurrió. Es el trade-off elegido frente a duplicar en silencio.
3. **Exactamente una respuesta entregada por pedido.** Gana la primera; el resto, `409 desconocido`.
4. **FIFO por instancia, no global.** Explícito, no un descuido: los reasignados vuelven al frente a
   propósito, y con consumidores que compiten no habría orden observable ni con una sola fila.
5. **Terminación.** Todo pedido aceptado termina en una respuesta antes de
   `presupuestoMs + COLA_INTERVALO_RECUPERADOR`. Excepción única: su instancia se murió — ahí lo cierra
   el timeout del balanceador (`PRESUPUESTO + GRACIA`) con un 504.
6. **La respuesta al destinatario se entrega a lo sumo una vez.** Si el balanceador muere entre el
   `tomar_respuesta` y el despacho, se pierde. Un peek+ack lo evitaría duplicando el protocolo para
   cubrir un caso en que el que esperaba ya no existe.
7. **Versionado.** Agregar un campo opcional a una entrada o un campo nuevo a una salida es v1. Quitar
   o renombrar un campo, cambiar qué significa un código, o volver obligatorio algo que no lo era, es
   v2 — y v1 y v2 conviven durante el despliegue.

### Lo que cambia para quien escribe el worker

Es el cambio mínimo posible, y hay que pasárselo por escrito:

1. La URL de la cola pasa a ser una **lista**.
2. **Un hilo de pull por instancia**, cada uno con el bucle de hoy sin tocar. Con un solo hilo contra
   una instancia, el trabajo encolado en las otras espera hasta que venza su presupuesto.
   Consecuencia aceptada: una réplica puede tener N pedidos a la vez — ya es un servidor gRPC
   concurrente y la reserva es por pedido.
3. **Contestar a la instancia de la que vino el pedido** (campo `cola`, o simplemente la URL que ese
   hilo está usando). Contestarle a otra da `409 desconocido` y el pedido se pierde hasta que venza.
4. `consumidor` sigue siendo el mismo string en los N hilos: el `host:puerto` gRPC de la réplica.

Las tres trampas de hoy siguen valiendo (`cola/README.md:254-269`): usar `quedaMs` como timeout,
devolver en `SIGTERM`, tratar el `409` como normal.

---

## Verificación

**Después de la etapa 2, sin desplegar nada nuevo** (la prueba de no-regresión):

```bash
./.venv/bin/python -m unittest discover -s tests -v          # todo verde, sin cambios
python3 consola.py                                            # BA_COLA_URL con una sola URL
curl -s localhost:8080/health | python3 -m json.tool          # cola.instancias con 1 elemento
```

**Anillo de 3, a mano:**

```bash
for p in 8085 8086 8087; do
  COLA_PUERTO=$p COLA_INSTANCIA=cola-$p python3 cola/servidor.py &
done
BA_COLA_URL=http://127.0.0.1:8085,http://127.0.0.1:8086,http://127.0.0.1:8087 \
  python3 app/balanceador.py
./.venv/bin/python app/verificador.py
```

Qué mirar: `/health` suma los tres `encolados`; con carga, los tres muestran movimiento; matar
`cola-8086` no baja el servicio y `cola.estado` pasa a `"degradada"`.

**Tests nuevos:**

- `tests/test_anillo.py` (repo del balanceador, con instancias de mentira):
  reparto round-robin; failover cuando una no conecta; **no** failover de un pedido no idempotente
  cuyo fallo fue leyendo la respuesta; todas llenas → 503 cola llena con los números sumados; ninguna
  responde → 503 cola caída; recolección en abanico (respuesta publicada en la instancia 3 despierta
  al handler); **503 de una instancia no produce bucle cerrado** (medir llamadas en 1 s).
- `tests/test_servidor_cola.py` (repo de la cola): se le agrega que el campo `cola` sale en el pedido
  y que responder a la instancia equivocada da `409 desconocido`. El test del 204 con `Content-Length: 0`
  seguido de otra request sobre la **misma** conexión keep-alive no se toca nunca: es el que cubre el
  `BadStatusLine` ya documentado en `cola/servidor.py:169-186`.
- `tests/test_colas.py`: sin cambios. Es la garantía de que la lógica de cola no se tocó.

**Caos, para el informe:**

| | Qué se hace | Qué tiene que pasar |
| :--- | :--- | :--- |
| 1 | matar una instancia con long-polls abiertos | el servicio sigue; sólo sus pedidos en vuelo salen 504; los recolectores de las otras no se enteran |
| 2 | levantarla de nuevo | vuelve al anillo sola por la histéresis, sin reiniciar nada |
| 3 | matar el worker con un pedido en vuelo | el idempotente reaparece con `intento=2`; el alta sale `DEADLINE_EXCEEDED` a los ~2 s |
| 4 | apagar las tres | 503 "el sistema de colas no responde", **no** "cola llena"; CPU del balanceador cerca de cero (`docker stats`), no al 100% |
