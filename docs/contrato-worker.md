# Contrato del worker — servicio de colas

> **Para el equipo que desarrolla el worker.** Contrato **v1**. Documento autocontenido: no hace
> falta leer nada más para implementar el worker.
>
> Lo que **no** está acá, a propósito: cómo está implementado el clúster de colas por dentro (Raft,
> log de replicación, elección de master). El worker no necesita saber nada de eso, y de hecho
> **no debe** depender de nada de eso. Si te interesa el diseño interno, está en
> `docs/plan-cola-desacoplada.md`, pero es lectura opcional.

## Estado de la implementación (leer esto primero)

| | Hoy | Objetivo del plan |
| :--- | :--- | :--- |
| Nodos de cola | **uno solo** | clúster de 3+ (impar) con master/slave |
| Rutas de datos | ya funcionan tal cual están acá | sin cambios |
| `409 no-soy-master` | nunca lo devuelve | lo devuelve cualquier nodo que no sea master |
| Descubrimiento | innecesario (una URL fija) | seed list + `/health` |

**Podés desarrollar y probar hoy mismo, en paralelo, sin esperar al clúster.** Si implementás el
worker como dice este documento y lo corrés contra el nodo único actual con una seed list de **un**
elemento, funciona igual: el nodo nunca contesta `409 no-soy-master`, así que la rama de
redescubrimiento simplemente no se ejecuta. Cuando el clúster esté, el worker ya está listo y no hay
que tocarlo.

Eso es justamente el objetivo de este contrato: que los dos desarrollos avancen sin bloquearse.

---

## Dónde encaja el worker

```
   Clientes HTTP
        │
        ▼
   Balanceador ──────────┐
   (no es tu problema)   │  publica pedidos, recolecta respuestas
                         ▼
              ┌──────────────────────┐
              │  Servicio de colas   │   ← con esto hablás vos
              │  (master + slaves)   │
              └──────────────────────┘
                         ▲
                         │  tomar pedido / devolver / responder
                         │
                    TU WORKER
                 (dentro de cada réplica)
```

Reglas de encuadre:

1. **El worker habla directo con el servicio de colas.** No pasa por el balanceador. No le pide
   permiso a nadie.
2. **El worker es un consumidor que compite.** Hay N réplicas con N workers tirando de la misma
   cola. Nadie asigna trabajo: el que está libre toma el próximo pedido.
3. **El worker nunca conoce al cliente final** ni le contesta. Publica la respuesta en la cola y el
   balanceador se la lleva.

---

## Configuración que necesita el worker

| Variable | Ejemplo | Qué es |
| :--- | :--- | :--- |
| Lista de nodos de cola | `http://100.101.15.93:8085,http://100.101.15.93:8086,http://100.101.15.93:8087` | La *seed list*. Separada por comas. **Con un solo elemento funciona igual** (situación de hoy) |
| Token de consumidor | `xY3k...` | Va en el header `X-Cola-Token` de **cada** request |
| Identidad del consumidor | `100.91.134.43:8080` | El `host:puerto` **gRPC de tu réplica**. Ver abajo por qué importa |

### Sobre `consumidor`: tiene que ser el `host:puerto` gRPC de la réplica

No es un nombre libre. Tiene que ser **el mismo string** que el balanceador usa como `destino` en su
registro de réplicas. Es lo que permite que el `/health` del balanceador cruce "esta réplica está
sana" con "esta réplica consumió 40 pedidos y tiene 2 en vuelo", sin traducir nada en el medio.

Si mandás otro string, la réplica va a figurar como sana y **sin consumir nada**, y alguien va a
perder una tarde buscando por qué.

El mismo string va como `atendidoPor` al responder.

---

## Autenticación

Todas las requests llevan el header:

```
X-Cola-Token: <el token de consumidor>
```

Sin token válido: `403 {"error": "token inválido"}`. El token de consumidor habilita sólo las rutas
del worker (`/pedidos/tomar`, `/pedidos/devolver`, `/respuestas`). Las rutas del balanceador y las
internas del clúster usan otros tokens: si recibís `403` en una ruta que no es tuya, es esperado.

---

## Descubrimiento del master

Sólo el nodo **master** atiende pedidos y respuestas. Los otros nodos (slaves) están para tomar la
posta si el master se cae, y **rechazan** cualquier operación de datos.

Dato importante para que esto no te parezca arbitrario: `tomar` un pedido **no es una lectura**, es
una mutación (reserva el pedido y lo saca del pool de disponibles). Por eso no se puede repartir
entre réplicas de lectura: si dos workers pudieran tomar contra dos nodos distintos, el mismo pedido
se entregaría dos veces.

### La idea: el clúster te dice quién es el master

Cualquier nodo sabe quién es el master, incluso un slave. Se lo preguntás con `GET /health`
(no lleva token):

```jsonc
// ← 200
{"cola": "sana",
 "rol": "slave",                              // "master" | "slave" | "candidato"
 "termino": 7,
 "masterConocido": "http://100.101.15.93:8086",  // null si hay elección en curso
 "contrato": "1.0",
 "instancia": "cola-8085@casa-tomas",
 "esperando": 3, "enVuelo": 2, "cota": 100}
```

### Arranque en frío

1. Tomá cualquier nodo de la seed list.
2. `GET /health` → leé `masterConocido`.
3. Cacheá esa URL. Usala para todo.

Si `masterConocido` viene `null` o el nodo no responde, probá el siguiente de la lista. Si ninguno da
un master, el clúster está en elección: **esperá con backoff y reintentá**. No hay nada mejor que
hacer — nadie puede inventar un master que todavía no fue electo.

### Régimen normal: costo cero

Con la URL cacheada, cada `tomar` y cada `responder` es un POST directo. **No preguntes `/health`
antes de cada operación**: el descubrimiento ocurre sólo al arrancar y cuando algo se rompe.

### El `409 no-soy-master`

Si le pegás a un nodo que no es el master (porque cambió y tu caché quedó viejo):

```jsonc
// ← 409
{"error": "no-soy-master", "master": "http://100.101.15.93:8086"}   // o "master": null
```

Qué hacer: actualizá el caché con esa URL y **reintentá una vez** ahí. Si viene `master: null`,
volvé a la seed list con backoff.

> ### ⚠ Colisión de códigos: hay dos `409` distintos
>
> `POST /respuestas` ya usaba `409` para otra cosa. **Distinguí por el cuerpo, no por el status:**
>
> | Cuerpo | Significa | Qué hacer |
> | :--- | :--- | :--- |
> | `{"error": "no-soy-master", ...}` | le pegaste al nodo equivocado | actualizar caché y reintentar |
> | `{"resultado": "desconocido"}` | el pedido ya lo contestó otro | **descartar, no reintentar** |
> | `{"resultado": "destinatario-saturado"}` | el balanceador no recolecta | descartar o reintentar más tarde |
>
> Confundir el primero con el segundo hace que descartes respuestas válidas. Chequeá la clave
> `error` antes que nada.
>
> **Decisión pendiente de la reunión**: proponemos cambiar `no-soy-master` a **`421 Misdirected
> Request`**, que semánticamente es exactamente eso ("le pediste a un servidor que no puede
> responder esto") y elimina la colisión. Si se aprueba, es el único cambio que impacta este
> documento. Codificá la detección por cuerpo y el cambio te sale gratis.

### Cuando cae el master

1. Tu long-poll se corta (connection reset/refused, o timeout).
2. **Probablemente todavía no haya master**: el clúster necesita detectar la caída y resolver una
   elección. Son cientos de milisegundos, no instantáneo.
3. Recorré la seed list con `/health` y **backoff** hasta que alguien diga `"rol": "master"`.
4. Actualizá el caché y reabrí el long-poll ahí.

Esto es operación normal, no un error a reportar. Lo único que **no** se puede hacer es reintentar en
bucle cerrado sin dormir: con el clúster caído, eso te pone la CPU al 100%.

---

## Las rutas que usa el worker

### `POST /pedidos/tomar` — llevarse el próximo (long-poll)

```jsonc
// →
{"consumidor": "100.91.134.43:8080",   // tu host:puerto gRPC
 "espera": 20}                          // segundos de long-poll; techo 30

// ← 200
{"id": "4b7baf0d2d4a4e909ec90c6f17690125",
 "operacion": "POST /personas",
 "parametros": {"nombre": "Ada", "legajo": 1234},
 "idempotente": false,
 "cliente": "100.118.61.111",   // informativo
 "quedaMs": 4870,               // presupuesto restante: USALO COMO TIMEOUT
 "intento": 1}                  // 2 o más = a este pedido ya lo abandonó otra réplica

// ← 204 sin cuerpo. No había trabajo en esos segundos. Volvé a llamar.
// ← 409 {"error": "no-soy-master", "master": "..."}
```

**No viaja ningún instante absoluto, sólo `quedaMs`.** Los relojes de las distintas máquinas no
están sincronizados: si el pedido llevara un `vence_en`, una máquina adelantada dos segundos
descartaría pedidos vivos. `quedaMs` se calcula con el reloj de la cola, que es el único que cuenta.

### `POST /respuestas` — contestar

```jsonc
// →
{"id": "4b7baf0d2d4a4e909ec90c6f17690125",   // el mismo del pedido
 "estado": "OK",                             // nombre del código de estado gRPC
 "contenido": {"servidoPor": "python-1", "persona": {"id": 7, "nombre": "Ada", "legajo": 1234}},
 "atendidoPor": "100.91.134.43:8080",        // el mismo string que `consumidor`
 "app": "python"}

// ← 202 {"resultado": "entregada"}
// ← 409 {"resultado": "desconocido"}           ya lo contestó otro: descartá, no reintentes
// ← 409 {"resultado": "destinatario-saturado"} el balanceador no está recolectando
// ← 409 {"error": "no-soy-master", "master": "..."}
```

**No mandes `destinatario`: lo pone la cola** con lo que guardó del pedido. El worker no tiene por
qué saber quién le pidió, y si se lo preguntáramos podría apuntar a otro balanceador y meterle una
respuesta ajena.

### `POST /pedidos/devolver` — soltarlo sin atenderlo

```jsonc
// →  {"id": "4b7baf0d…", "consumidor": "100.91.134.43:8080"}
// ← 200 {"resultado": "devuelto"}
// ← 409 {"resultado": "no-estaba-en-vuelo"}   la reserva ya venció; no hagas nada
// ← 409 {"error": "no-soy-master", "master": "..."}
```

Para cuando la réplica se apaga: devolvé lo que tenés en la mano en vez de hacer esperar los
segundos de la reserva. Es una optimización, no una garantía — si no llegás a devolverlo, el
recuperador de la cola lo hace igual, sólo más tarde.

### `GET /health` — descubrimiento y diagnóstico

Sin token. Ver arriba.

---

## Las operaciones a resolver

`operacion` es opaco para la cola y mapea 1:1 con un RPC de `contrato.proto`:

| `operacion` | `parametros` | RPC | `contenido` de la respuesta |
| :--- | :--- | :--- | :--- |
| `GET /` | `{}` | `Identidad` | `{app, lenguaje, equipo: [{nombre, apellido, legajo}], version, mensaje, host, arrancado, servidoPor}` |
| `POST /echo` | `{ping}` | `Echo` | `{pong, servidoPor, version}` |
| `GET /personas` | `{}` | `ListarPersonas` | `{servidoPor, personas: [{id, nombre, legajo}]}` |
| `POST /personas` | `{nombre, legajo}` | `CrearPersona` | `{servidoPor, persona: {id, nombre, legajo}}` |

- `legajo` llega **siempre como entero**: el balanceador lo normaliza antes de encolar.
- Cuando el RPC falla: `contenido` es `{"error": "<detalle>"}` y `estado` es el nombre del código
  gRPC recibido (`INVALID_ARGUMENT`, `ALREADY_EXISTS`, `NOT_FOUND`, …). No traduzcas a HTTP: de eso
  se encarga el balanceador.
- **El `contenido` lo arma el worker**, no el balanceador. Agregar un campo a la app no obliga a
  tocar el balanceador.

---

## Garantías, y qué significan para vos

1. **`idempotente: true` → al-menos-una-vez.** Tu worker **tiene que tolerar re-ejecución**. El
   campo `intento` dice cuántas veces se entregó antes: `intento >= 2` significa que otra réplica ya
   lo tuvo y no contestó.
2. **`idempotente: false` → a-lo-sumo-una-entrega.** Si se te vence la reserva, el pedido **no** se
   re-entrega a nadie: se falla con `DEADLINE_EXCEEDED`. Consecuencia honesta: si tu worker alcanzó a
   escribir en la base y murió antes de contestar, el cliente ve un error sobre un alta que **sí
   ocurrió**. Es el trade-off elegido frente a duplicar altas en silencio.
3. **Exactamente una respuesta entregada por pedido.** Gana la primera. Al resto le llega
   `409 desconocido`, y eso es normal.
4. **Un pedido confirmado no se pierde si se cae un nodo de la cola.** La cola no confirma nada hasta
   que la mayoría del clúster lo tiene. No es algo que tengas que hacer vos, pero es bueno saber que
   el pedido que tenés en la mano sobrevive a la caída de un nodo.
5. **Terminación.** Todo pedido termina en una respuesta antes de que se agote su presupuesto. Si te
   pasás de `quedaMs`, el pedido ya no es tuyo.

---

## Reglas duras: lo que el worker NO debe hacer

- **No implementes nada de Raft.** No votes, no te preocupes por términos, no consultes `/raft/*`
  (están protegidas con otro token y son sólo entre nodos de la cola). Tu vista del mundo es: "hay
  una cola, a veces cambia de dirección y me avisan con un `409`".
- **No le pegues a un slave a propósito** para repartir carga. `tomar` muta estado: no es una
  lectura y no se puede replicar entre réplicas de lectura.
- **No reintentes a ciegas un POST que pudo haber llegado.** Si el error ocurrió *leyendo la
  respuesta*, no sabés si el servidor la procesó. Reintentar un `POST /respuestas` en ese caso es
  inofensivo (el segundo da `409 desconocido`), pero reintentar un `tomar` te puede reservar **dos**
  pedidos sin que te des cuenta de que tenés dos.
- **No uses un `consumidor` inventado.** Ver arriba.
- **No mandes `destinatario`** en la respuesta.
- **No reintentes en bucle cerrado.** Siempre con backoff.

---

## El worker completo, en pseudocódigo

```python
NODOS = os.environ["COLA_URLS"].split(",")      # seed list
TOKEN = os.environ["COLA_TOKEN_CONSUMIDOR"]
MI_DESTINO = os.environ["MI_HOST_PUERTO_GRPC"]  # "100.91.134.43:8080"

master = None          # caché

def descubrir_master():
    """Recorre la seed list hasta encontrar quién es master. None si hay elección."""
    for url in NODOS:
        try:
            salud = get(f"{url}/health")
            if salud["rol"] == "master":
                return url
            if salud.get("masterConocido"):
                return salud["masterConocido"]
        except ConnectionError:
            continue
    return None

def pedir(ruta, cuerpo):
    """POST al master, siguiendo el redirect si el caché quedó viejo."""
    global master
    for _ in range(2):                       # una sola redirección
        if master is None:
            master = descubrir_master()
            if master is None:
                dormir_con_backoff()         # clúster en elección
                continue
        try:
            codigo, respuesta = post(f"{master}{ruta}", cuerpo,
                                     headers={"X-Cola-Token": TOKEN})
        except ConnectionError:
            master = None                    # se cayó: redescubrir
            continue

        if codigo == 409 and respuesta.get("error") == "no-soy-master":
            master = respuesta.get("master")  # puede ser None
            continue                          # ojo: chequear `error`, no el status
        return codigo, respuesta
    return None, None

while True:
    codigo, pedido = pedir("/pedidos/tomar", {"consumidor": MI_DESTINO, "espera": 20})

    if codigo is None:            # clúster sin master; ya se durmió con backoff
        continue
    if codigo == 204:             # no había trabajo; el long-poll ya esperó
        continue

    timeout = pedido["quedaMs"] / 1000        # NO usar un timeout fijo
    try:
        contenido = RESOLVER[pedido["operacion"]](pedido["parametros"], timeout)
        estado = "OK"
    except grpc.RpcError as e:
        estado, contenido = e.code().name, {"error": e.details()}

    pedir("/respuestas", {"id": pedido["id"], "estado": estado, "contenido": contenido,
                          "atendidoPor": MI_DESTINO, "app": "python"})
    # un 409 "desconocido" acá es normal: llegaste tarde y otro ya contestó
```

Al recibir `SIGTERM`: `POST /pedidos/devolver` con lo que tengas en la mano, después salir.

---

## Las trampas (se pagan caras)

1. **Usar `quedaMs` como timeout del RPC.** Un worker que ignora el presupuesto sigue trabajando en
   un pedido que la cola ya reasignó: dos réplicas haciendo el mismo trabajo y, en un alta, dos
   personas.
2. **Devolver lo que se tiene en la mano al apagarse** (`SIGTERM` → `/pedidos/devolver`). Sin eso,
   cada despliegue le cuesta segundos de espera a un puñado de pedidos.
3. **Tratar el `409 desconocido` como normal.** Llega cuando la réplica tardó más que la reserva. No
   es un error del worker y no hay que reintentar.
4. **Distinguir los dos `409` por el cuerpo.** Ver la caja de arriba. Es la trampa nueva de esta
   versión del contrato.
5. **Un hilo de pull, no uno por nodo.** Hay una sola cola lógica: sólo el master reparte trabajo.
   Abrir un hilo por nodo de la seed list no te da más throughput, te da `409` de los slaves.

---

## Checklist de aceptación

Antes de decir "está listo":

- [ ] Arranca con la seed list y encuentra el master sin configuración extra.
- [ ] Con una seed list de **un** elemento (el nodo único de hoy) funciona igual.
- [ ] Usa `quedaMs` como timeout del RPC, no un valor fijo.
- [ ] `consumidor` y `atendidoPor` son el `host:puerto` gRPC de la réplica, y **el mismo string**.
- [ ] `204` no se trata como error: vuelve a llamar.
- [ ] Los dos `409` se distinguen **por el cuerpo** y se manejan distinto.
- [ ] Ante corte de conexión, redescubre el master con backoff y reengancha **sin reiniciar el
      proceso**.
- [ ] Con el clúster entero caído, la CPU del worker queda cerca de cero (backoff real, no bucle).
- [ ] `SIGTERM` devuelve el pedido en vuelo antes de salir.
- [ ] `intento >= 2` no rompe nada en operaciones idempotentes.
- [ ] No hay ninguna referencia a `/raft/*` ni lógica de términos/elección en el código.

---

## Versionado del contrato

`GET /health` devuelve `"contrato": "1.0"`. Declará contra qué versión programaste y compará el
**major** al arrancar: si difiere, fallá ruidosamente en vez de operar con un contrato desconocido.

- **Sigue siendo v1**: agregar un campo opcional a una entrada, o un campo nuevo a una salida.
- **Pasa a v2**: quitar o renombrar un campo, cambiar qué significa un código, o volver obligatorio
  algo que no lo era. v1 y v2 conviven durante el despliegue.

Las rutas **no** llevan prefijo de versión (`/pedidos/tomar`, no `/v1/pedidos/tomar`): la versión se
declara en `/health`. Así, subir de versión no obliga a reconfigurar la URL del worker.

---

## Contacto y cambios

Cualquier cosa de este contrato que no cierre, preguntala **antes** de implementar: un supuesto
distinto de los dos lados sale carísimo. La única decisión abierta hoy es la del `421` vs `409` para
`no-soy-master`, marcada arriba.
