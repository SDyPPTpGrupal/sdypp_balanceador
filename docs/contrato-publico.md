# Contrato público

> **Para quien quiera usar el servicio desde afuera.** Todo lo que hace falta está acá: la URL, las
> cinco operaciones y la forma de las respuestas. No hay que instalar ni registrar nada, y no lleva
> token: el puerto público no pide autenticación.

**URL:** `https://tomas.tail93cadf.ts.net`

Es una PC del grupo publicada con Tailscale Funnel, con certificado válido, así que se llega desde
cualquier lado con `curl`, el navegador o Postman. Detrás hay un balanceador, un clúster de colas
replicado con Raft en tres PCs distintas y las réplicas de la app (Python y Java).

## El sobre

**Toda** respuesta —salga bien o mal— tiene la misma forma:

```json
{"Code": 200, "contenido": { ... }}
{"Code": 404, "contenido": {"error": "no existe"}}
```

`Code` repite el código HTTP y `contenido` es siempre un objeto: el payload si salió bien, o
`{"error": "..."}` si no.

## Las operaciones

| Método y ruta | Qué hace |
| :--- | :--- |
| `GET /` | Identidad de la réplica que atendió |
| `GET /health` | Salud del servicio entero |
| `POST /echo` | Devuelve lo que se le manda |
| `GET /personas` | Lista las personas |
| `POST /personas` | Crea una persona |
| `GET /pedidos/<ticket>` | Retira la respuesta de un pedido asincrónico |

### `GET /` — identidad

```bash
curl -s https://tomas.tail93cadf.ts.net/
```

```json
{"Code": 200, "contenido": {
  "app": "sdypp-python", "lenguaje": "python", "version": 3,
  "equipo": [{"nombre": "...", "apellido": "...", "legajo": 0}],
  "mensaje": "...", "host": "...", "arrancado": "...", "servidoPor": "sdypp-python"}}
```

`host` y `servidoPor` dicen qué réplica atendió. Repitiendo el pedido se ve el reparto entre ellas.

### `POST /echo`

```bash
curl -s -X POST https://tomas.tail93cadf.ts.net/echo \
  -H 'Content-Type: application/json' -d '{"ping": "hola"}'
```

```json
{"Code": 200, "contenido": {"pong": "hola", "servidoPor": "sdypp-python", "version": 3}}
```

### `GET /personas`

```bash
curl -s https://tomas.tail93cadf.ts.net/personas
```

```json
{"Code": 200, "contenido": {"servidoPor": "sdypp-python",
  "personas": [{"id": 1, "nombre": "Ada", "legajo": 1234}]}}
```

### `POST /personas`

```bash
curl -s -X POST https://tomas.tail93cadf.ts.net/personas \
  -H 'Content-Type: application/json' -d '{"nombre": "Ada", "legajo": 1234}'
```

```json
{"Code": 201, "contenido": {"servidoPor": "sdypp-python",
  "persona": {"id": 7, "nombre": "Ada", "legajo": 1234}}}
```

`legajo` tiene que ser un entero. Es la única escritura, y es la única operación que **no** se
reintenta sola: si la réplica que la tomó se muere, el pedido se falla en vez de reasignarse, para
no crear la persona dos veces.

### `GET /health`

```bash
curl -s https://tomas.tail93cadf.ts.net/health
```

```json
{"Code": 200, "contenido": {
  "balanceador": "sano",
  "casa": "casa-tomas",
  "replicas": ["100.91.134.43:8080", "100.101.15.93:8111"],
  "cola": {"estado": "sana", "encolados": 0, "enVuelo": 1, "cota": 1000,
           "nodos": [{"url": "http://100.78.246.64:8085", "rol": "master", "termino": 8,
                      "instancia": "cola-mateo"},
                     {"url": "http://100.91.134.43:8085", "rol": "slave", "termino": 8,
                      "instancia": "cola-salva"},
                     {"url": "http://100.120.186.92:8085", "rol": "caido"}]}}}
```

Contesta una sola pregunta: **¿se puede atender ahora mismo?** Para eso hacen falta dos cosas, y
`200` significa que están las dos:

- la cola está viva (`cola.estado` en `sana`; `eligiendo` es un cambio de líder en curso, `caída` es
  que no contesta ningún nodo);
- hay al menos una réplica consumiendo trabajo: es la lista `replicas`, que sale de quién fue a
  buscar pedidos en los últimos 45 s, no de un registro estático.

Si falta alguna de las dos, es `503`. El clúster tolera que se caiga un nodo de los tres: con dos
vivos sigue habiendo mayoría y el servicio no se corta.

## Sin quedarse esperando

Por defecto el cliente espera la respuesta en la misma conexión, hasta 5 segundos. Mandando
`Prefer: respond-async` (RFC 7240) en cualquiera de las cuatro operaciones, la respuesta es
inmediata: un ticket que se retira después.

```bash
curl -si -X POST https://tomas.tail93cadf.ts.net/personas \
  -H 'Content-Type: application/json' -H 'Prefer: respond-async' \
  -d '{"nombre": "Ada", "legajo": 1234}'
```

```
HTTP/1.1 202 Accepted
Location: /pedidos/4b7baf0d….1790058626
Retry-After: 1
Preference-Applied: respond-async

{"Code": 202, "contenido": {"id": "4b7baf0d….1790058626", "estado": "pendiente"}}
```

```bash
curl -s https://tomas.tail93cadf.ts.net/pedidos/4b7baf0d….1790058626
```

| Respuesta | Qué significa |
| :--- | :--- |
| `202 {"estado": "pendiente"}` | Todavía no está: volver a consultar en `Retry-After` |
| `200` · `201` · `400` · `409` · `504` | Lo mismo que habría contestado el camino sincrónico |
| `404` | Ticket mal formado, o vencido: pasados 120 s ya no existe |

Dos cosas del ticket:

- **se entrega una sola vez.** Retirar la respuesta la saca de la cola, así que hay que quedarse con
  lo que llega; una segunda consulta dice `pendiente` hasta que el ticket vence;
- el pedido puede esperar hasta 60 s por una réplica libre, en vez de los 5 del camino sincrónico.

El balanceador no guarda el ticket en ningún lado: la respuesta queda en la cola, replicada como
cualquier otro dato, así que sobrevive a un cambio de líder y a un reinicio del balanceador.

## Códigos

| Código | Cuándo |
| :--- | :--- |
| `200` · `201` | Bien. `201` es el alta |
| `202` | Ticket pendiente |
| `400` | Cuerpo que no es JSON, `legajo` que no es entero, o nombre o legajo inválidos |
| `404` | Ruta que no existe, o ticket desconocido o vencido |
| `409` | Ese legajo ya está registrado |
| `502` | La cola rechazó el pedido |
| `503` | No hay quién atienda: cola caída, cola llena, ninguna réplica consumiendo, o la réplica se quedó sin su base |
| `504` | Ninguna réplica lo atendió dentro del presupuesto (5 s, o 60 s con ticket) |
