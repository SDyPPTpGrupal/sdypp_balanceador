#!/usr/bin/env python3
"""Balanceador de la App — HTTP afuera, la cola adentro de la red.

Es la única URL pública del servicio. Recibe HTTP/JSON (el contrato del
enunciado: GET /, GET /health, POST /echo, GET /personas, POST /personas),
publica cada pedido en el **sistema de colas** y espera su respuesta. No elige
réplica, y desde este refactor tampoco le habla a ninguna por el plano de datos:
los workers viven adentro de las réplicas y van a buscar trabajo a la cola.

    cliente ──HTTP──▶ balanceador ──POST /pedidos──▶ ┌── cola de pedidos ───┐
                           ▲                          │  (sistema aparte)   │
                           └──POST /respuestas/tomar──┤ cola de respuestas  │
                                                      └──────────▲──────────┘
                                          réplicas ──toman pedido─┘ y responden

Qué queda del balanceador después de sacarle los workers:

  * el contrato público y su sobre (lo único que ve el cliente);
  * la traducción request HTTP → pedido JSON → respuesta HTTP;
  * el pool de réplicas y el vigilante de salud, que ya **no rutean** nada: son
    lo que /health y el CD miran para saber qué réplicas hay y si están vivas.

El contrato con el cliente no cambió: **toda** respuesta del plano público sale
con la misma forma, salga bien o mal.

    {"Code": 200, "contenido": {"app": "python", "version": 3, ...}}
    {"Code": 404, "contenido": {"error": "no existe"}}

`Code` repite el código HTTP y `contenido` es siempre un objeto. El sobre lo
pone `Manejador.responder`, no cada handler: es la única forma de garantizar que
no se escape ninguna respuesta por un camino de error que nadie probó.

    python3 app/balanceador.py

    BA_PUERTO=8080 BA_COLA_URL=http://127.0.0.1:8085 python3 app/balanceador.py

Por qué traduce en vez de reenviar bytes: si sólo pasáramos paquetes (proxy L4)
no sabríamos qué operación pasó, y el enunciado pide una línea de bitácora por
request diciendo qué se hizo con ella. Para escribir esa línea hay que entender
el pedido. El precio es que el balanceador conoce el contrato.
"""

import json
import os
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clientecola import ErrorCola
from clientereplica import ClienteReplica

# --- Configuración ---------------------------------------------------------
# Todo por entorno: la topología se decide el día de la demo sin tocar código.

PUERTO = int(os.environ.get("BA_PUERTO", "8080"))
CASA = os.environ.get("BA_CASA", "casa-tomas")
NOMBRE = os.environ.get("BA_NOMBRE", "balanceador")

# Quién es este balanceador para la cola. Es el `destinatario` de sus pedidos:
# la cola guarda las respuestas bajo este nombre y sólo se las entrega a quien
# lo pida. Con dos balanceadores (Etapa 3) cada uno recolecta lo suyo y ninguno
# se lleva la respuesta que el otro está esperando.
IDENTIDAD = os.environ.get("BA_IDENTIDAD", f"{NOMBRE}@{CASA}")

# El sistema de colas. Acepta una lista separada por comas (seed list del clúster).
COLA_URL = os.environ.get("BA_COLA_URL", "http://127.0.0.1:8085")
COLA_URLS = [u.strip() for u in COLA_URL.split(",") if u.strip()]
COLA_TOKEN = os.environ.get("BA_COLA_TOKEN", "")

# Backends iniciales, separados por coma: "salvador:8080,mateon:8080".
# Puede quedar vacío: el CD los va cargando por /admin/backends.
BACKENDS_INICIALES = os.environ.get("BA_BACKENDS", "")

DIRECTORIO_LOGS = os.environ.get("BA_LOGS", "logs")
ARCHIVO_BITACORA = os.path.join(DIRECTORIO_LOGS, f"bitacora-{NOMBRE}-{CASA}.log")

# Cada cuánto le preguntamos a cada réplica si sigue viva, y cuántas respuestas
# malas seguidas hacen falta para sacarla. Más de una porque un timeout aislado
# es normal en una red doméstica.
#
# Ojo con lo que significa ahora: marcarla caída ya no saca tráfico de ningún
# lado, porque el tráfico lo toma ella de la cola. Sirve para que /health diga
# la verdad y para que el CD sepa qué encontró, nada más.
INTERVALO_SALUD = float(os.environ.get("BA_INTERVALO_SALUD", "3"))
FALLOS_PARA_SACAR = int(os.environ.get("BA_FALLOS_PARA_SACAR", "2"))
EXITOS_PARA_VOLVER = int(os.environ.get("BA_EXITOS_PARA_VOLVER", "1"))
TIMEOUT_SALUD = float(os.environ.get("BA_TIMEOUT_SALUD", "2"))

# Presupuesto total de un pedido, espera en cola incluida. Un solo número y no
# "timeout de cola + timeout de atención": lo que le importa al cliente es
# cuánto tarda la respuesta, no en qué parte del camino se fue el tiempo.
PRESUPUESTO = float(os.environ.get("BA_PRESUPUESTO", os.environ.get("BA_TIMEOUT_RPC", "5")))

# Cuánto más que el presupuesto espera el handler antes de rendirse solo. La
# cola ya devuelve un DEADLINE_EXCEEDED cuando un pedido vence, así que este
# margen es la red de contención para el caso en que la cola *misma* no
# conteste. Si salta este timeout y no el de la cola, el problema es la cola.
GRACIA = float(os.environ.get("BA_GRACIA_COLA", "1"))

# Hilos que recolectan respuestas. Cada uno tiene un long-poll abierto contra la
# cola; con uno solo, todas las respuestas del servicio pasarían por un único
# round-trip serializado y ése sería el techo de throughput.
RECOLECTORES = int(os.environ.get("BA_RECOLECTORES", "4"))
ESPERA_RECOLECTOR = float(os.environ.get("BA_ESPERA_RECOLECTOR", "20"))

# Hace cuánto tiene que haber ido a buscar trabajo una réplica para contarla como
# "consumiendo". Tiene que ser mayor que el long-poll de los workers (que lo
# decide cada réplica, con techo COLA_ESPERA_MAXIMA=30 s): un worker sin trabajo
# está bloqueado adentro de `tomar` y no vuelve a aparecer hasta que se le vence
# la espera. Con un umbral más corto, un sistema ocioso se vería como caído.
UMBRAL_CONSUMO = float(os.environ.get("BA_UMBRAL_CONSUMO", "45"))
# Cuánto espera un recolector antes de volver a intentar con la cola caída.
ESPERA_REINTENTO = float(os.environ.get("BA_ESPERA_REINTENTO", "1"))

# --- El plano de control, separado del plano de datos -----------------------
# /admin/backends lo consume el CD: quien lo toca decide qué réplicas figuran en
# el sistema. Por eso no vive en el puerto público sino en un socket propio, y
# ese socket escucha por defecto sólo en loopback: lo que no escucha en la red
# no se puede atacar desde la red.
PUERTO_ADMIN = int(os.environ.get("BA_PUERTO_ADMIN", "8081"))
ADMIN_BIND = os.environ.get("BA_ADMIN_BIND", "127.0.0.1")

# Segunda barrera: qué IPs pueden usarlo, ya habiendo llegado al socket.
# Vacío = sólo loopback.
ADMIN_PERMITIDOS = [x.strip() for x in os.environ.get("BA_ADMIN_IPS", "").split(",") if x.strip()]


def _ahora_iso():
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


_LOCK_BITACORA = threading.Lock()


def bitacora(operacion, codigo, destino=None, detalle=None):
    """Una línea por request, diciendo quién la terminó atendiendo.

    Mismo formato que la bitácora de las réplicas y la de la cola, a propósito:
    es lo que permite tomar un alta del verificador y seguirla por tres archivos
    en dos casas distintas con el mismo `req=`.
    """
    partes = [
        _ahora_iso(),
        f"{NOMBRE}@{CASA}",
        operacion,
        str(codigo),
        " ".join(x for x in (f"destino={destino}" if destino else None, detalle) if x) or "-",
    ]
    linea = " | ".join(partes)
    try:
        with _LOCK_BITACORA:
            os.makedirs(DIRECTORIO_LOGS, exist_ok=True)
            with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
                f.write(linea + "\n")
    except OSError as e:
        print(f"[bitacora] no se pudo escribir: {e}", flush=True)
    print(linea, flush=True)


# --- El pool ---------------------------------------------------------------

class Backend:
    """Una réplica conocida, con su estado de salud.

    Ya no guarda un stub del servicio: el balanceador no le hace ningún RPC de
    negocio. El único canal que abre es para `grpc.health.v1.Health`, que es lo
    que contesta la pregunta "¿esta réplica está viva?" sin inventar un endpoint
    nuevo ni tocar el contrato.
    """

    def __init__(self, destino, app="python"):
        self.destino = destino
        self.app = app
        self.sano = False          # arranca caído: entra en la lista de sanos
        self.fallos = 0            # cuando el chequeo lo confirma, no por confiar
        self.exitos = 0
        self.visto = None
        # Un canal por backend, reusado en cada chequeo: abrir uno por chequeo
        # tiraría a la basura el handshake cada tres segundos.
        self.canal = grpc.insecure_channel(destino)
        self.stub_salud = health_pb2_grpc.HealthStub(self.canal)

    def chequear(self):
        """Le pregunta por grpc.health.v1.Health. Devuelve si cambió de estado."""
        try:
            r = self.stub_salud.Check(
                health_pb2.HealthCheckRequest(service=""), timeout=TIMEOUT_SALUD
            )
            vivo = r.status == health_pb2.HealthCheckResponse.SERVING
        except grpc.RpcError:
            vivo = False
        self.visto = _ahora_iso()
        return self._registrar(vivo)

    def _registrar(self, vivo):
        antes = self.sano
        if vivo:
            self.fallos = 0
            self.exitos += 1
            if not self.sano and self.exitos >= EXITOS_PARA_VOLVER:
                self.sano = True
        else:
            self.exitos = 0
            self.fallos += 1
            if self.sano and self.fallos >= FALLOS_PARA_SACAR:
                self.sano = False
        return antes != self.sano

    def cerrar(self):
        self.canal.close()

    def como_json(self):
        return {
            "destino": self.destino,
            "app": self.app,
            "sano": self.sano,
            "fallos": self.fallos,
            "ultimoChequeo": self.visto,
        }


class Pool:
    """La lista de réplicas conocidas y su salud.

    Con los workers afuera, el pool dejó de decidir a dónde va el tráfico: una
    réplica atiende porque su worker consume de la cola, no porque este objeto
    la tenga anotada. Sigue existiendo por dos razones concretas:

      * es lo que el CD agrega y quita al conmutar blue/green, y esa interfaz
        (`POST /admin/backends`) la consume otro repo: sacarla sería romperlo;
      * es lo que hace que /health pueda decir "4 de 4 sanas" en vez de sólo
        "hay 3 pedidos en la cola".

    Qué implica que ya no rutee, y hay que decirlo en el informe: **quitar una
    réplica del pool no la deja de usar**. Para sacarla de circulación hay que
    apagar su worker, que es lo que hace el agente cuando baja el contenedor
    blue. El pool pasó de ser el interruptor a ser el registro.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._backends = {}

    def agregar(self, destino, app="python"):
        """Devuelve el backend recién creado, o None si el destino ya estaba."""
        with self._lock:
            if destino in self._backends:
                return None
            backend = Backend(destino, app)
            self._backends[destino] = backend
        return backend

    def quitar(self, destino):
        with self._lock:
            backend = self._backends.pop(destino, None)
        if backend is None:
            return False
        backend.cerrar()
        return True

    def todos(self):
        with self._lock:
            return list(self._backends.values())


def normalizar_destino(x):
    """Un elemento de `agregar`/`quitar` → (destino, app).

    Se aceptan las dos formas: "host:puerto" y {"destino": ..., "app": ...}. La
    corta asume Python por compatibilidad con el CD viejo, que mandaba strings
    pelados; por eso el CD manda hoy la forma larga, o las réplicas Java
    figurarían como Python en /health.
    """
    if isinstance(x, str):
        return x, "python"
    if isinstance(x, dict) and x.get("destino"):
        return x["destino"], x.get("app") or "python"
    return None, None


POOL = Pool()
CLIENTE = ClienteReplica(COLA_URLS, token=COLA_TOKEN)


def vigilar_salud():
    """Le pregunta a cada réplica cada INTERVALO_SALUD segundos.

    Preguntando y no esperando a que una request falle: antes el fallo de un RPC
    nos avisaba de la caída, pero ahora el balanceador no hace RPCs de negocio,
    así que **éste es el único aviso que hay**. Si este hilo se muere, /health
    miente y nadie se entera.
    """
    while True:
        for b in POOL.todos():
            try:
                if b.chequear():
                    estado = "vuelve a estar sana" if b.sano else "se cayó"
                    bitacora("salud", "CAMBIO", b.destino, estado)
            except Exception as e:
                print(f"[salud] {b.destino}: {e}", flush=True)
        time.sleep(INTERVALO_SALUD)


# --- El puente entre el handler y la cola -----------------------------------

CODIGOS = {
    "OK": 200,
    "INVALID_ARGUMENT": 400,
    "NOT_FOUND": 404,
    "ALREADY_EXISTS": 409,
    "PERMISSION_DENIED": 403,
    "UNAUTHENTICATED": 401,
    "DEADLINE_EXCEEDED": 504,
    "UNAVAILABLE": 503,
    "UNIMPLEMENTED": 501,
    "INTERNAL": 500,
}


class Espera:
    """Un handler bloqueado esperando la respuesta de su pedido.

    El hilo que atiende el HTTP no lee de la cola: si cada handler hiciera su
    propio long-poll se llevaría respuestas de otros y habría que devolverlas.
    En vez de eso deja esto anotado en `ESPERAS` y se duerme; los recolectores
    reparten por `id`.
    """

    __slots__ = ("listo", "respuesta")

    def __init__(self):
        self.listo = threading.Event()
        self.respuesta = None


ESPERAS = {}
_LOCK_ESPERAS = threading.Lock()


def recolectar(sigo_vivo=lambda: True):
    """Trae respuestas de la cola y despierta al handler que corresponda.

    Una respuesta sin nadie esperándola no es un error: es la que llegó después
    de que su handler se rindió, o la segunda de un pedido que dos réplicas
    atendieron. Se registra y se tira — despertar a nadie es exactamente lo que
    hay que hacer con ella.

    `sigo_vivo` existe para poder pararlo desde una prueba sin matar el proceso;
    en producción siempre da True y el hilo es daemon.
    """
    while sigo_vivo():
        try:
            codigo, respuesta = CLIENTE.tomar_respuesta(IDENTIDAD, ESPERA_RECOLECTOR)
        except ErrorCola as e:
            # La cola no responde. No se reintenta en bucle cerrado: sería
            # golpear un servicio caído miles de veces por segundo.
            print(f"[recolector] la cola no responde: {e}", flush=True)
            time.sleep(ESPERA_REINTENTO)
            continue
        if codigo in (503, 421):
            time.sleep(ESPERA_REINTENTO)
            continue
        if codigo == 204 or not respuesta:
            continue
        id = respuesta.get("id")
        with _LOCK_ESPERAS:
            espera = ESPERAS.pop(id, None)
        if espera is None:
            bitacora("recolectar", "TARDE", None,
                     f"req={id} llegó sin nadie esperándola ({respuesta.get('estado')})")
            continue
        espera.respuesta = respuesta
        espera.listo.set()


def _camino(respuesta):
    """`intentos=a→b ` cuando el pedido pasó por más de una réplica: la evidencia
    de la reasignación, en la misma línea de bitácora que el resultado."""
    intentos = respuesta.get("intentos") or []
    return f"intentos={'→'.join(intentos)} " if len(intentos) > 1 else ""


def derivar(operacion, parametros=None, idempotente=True, cliente=None):
    """Publica el pedido en la cola y espera su respuesta.

    Devuelve (codigo_http, contenido, atendido_por, detalle). `codigo_http` es
    None si salió bien y `contenido` es el payload que armó la réplica: el
    balanceador no lo reescribe, sólo lo mete en el sobre. Que la forma del
    payload la decida quien tiene los datos es lo que hace que agregar un campo
    a la app no obligue a tocar el balanceador.
    """
    id = uuid.uuid4().hex
    req = f"req={id}"
    espera = Espera()
    with _LOCK_ESPERAS:
        ESPERAS[id] = espera
    try:
        pedido = {
            "id": id,
            "operacion": operacion,
            "parametros": parametros or {},
            "idempotente": idempotente,
            "destinatario": IDENTIDAD,
            "cliente": cliente,
            "presupuestoMs": int(PRESUPUESTO * 1000),
        }
        try:
            codigo, datos = CLIENTE.publicar_pedido(pedido)
        except ErrorCola as e:
            # Sin cola no hay servicio: es el punto único de falla que este
            # diseño agrega y que hay que nombrar en el informe.
            return 503, {"error": "el sistema de colas no responde"}, None, f"cola caída: {e} {req}"
        if codigo == 503:
            err_msg = datos.get("error") if isinstance(datos, dict) else None
            if err_msg == "el sistema de colas no responde":
                return 503, {"error": "el sistema de colas no responde"}, None, f"cola sin líder: {req}"
            return (503, {"error": "cola llena"}, None,
                    f"cola llena ({datos.get('esperando') if isinstance(datos, dict) else '?'}/{datos.get('cota') if isinstance(datos, dict) else '?'}) {req}")
        if codigo != 202:
            err_str = datos.get('error', '') if isinstance(datos, dict) else ''
            return (502, {"error": "la cola rechazó el pedido"}, None,
                    f"HTTP {codigo} {err_str} {req}")

        if not espera.listo.wait(timeout=PRESUPUESTO + GRACIA):
            # La cola tendría que haber devuelto un DEADLINE_EXCEEDED al vencer
            # el presupuesto. Que salte este timeout quiere decir que la cola
            # dejó de contestar con el pedido adentro.
            return (504, {"error": "sin respuesta a tiempo"}, None,
                    f"venció sin noticias de la cola {req}")

        r = espera.respuesta
        estado = r.get("estado", "INTERNAL")
        atendido = r.get("atendidoPor")
        detalle = f"{_camino(r)}espera={r.get('esperaMs', '?')}ms {req}"
        if estado == "OK":
            return None, r.get("contenido") or {}, atendido, detalle
        return (CODIGOS.get(estado, 500), r.get("contenido") or {"error": estado},
                atendido, f"{estado} {detalle}")
    finally:
        with _LOCK_ESPERAS:
            ESPERAS.pop(id, None)


def consume(hace_ms):
    """¿Este consumidor fue a buscar trabajo hace poco?"""
    return hace_ms is not None and hace_ms <= UMBRAL_CONSUMO * 1000


def backends_json():
    """El pool cruzado con lo que sabe la cola de cada réplica.

    `enVuelo` y `atendidos` ya no los puede contar el balanceador: los pedidos
    no pasan por él. Los cuenta la cola, indexados por el `consumidor` con el
    que cada worker se identifica, que es el mismo `host:puerto` que usamos como
    `destino` acá. Que los dos lados usen el mismo string es lo que permite
    cruzarlos sin traducir.

    Devuelve (lista, estado_de_la_cola). El estado puede ser None: la cola está
    caída y entonces sólo se informa lo que el vigilante sabe por su cuenta.
    """
    cola = CLIENTE.estado()
    consumidores = (cola or {}).get("consumidores", {})
    salida = []
    for b in POOL.todos():
        d = b.como_json()
        visto = consumidores.get(b.destino)
        d["enVuelo"] = visto["enVuelo"] if visto else 0
        d["atendidos"] = visto["atendidos"] if visto else 0
        # "consumiendo" es lo que reemplaza al viejo campo "worker": ya no
        # miramos el estado de un hilo nuestro sino si esa réplica fue a buscar
        # trabajo hace poco. Una réplica sana que no consume es el síntoma nuevo
        # de este diseño —el contenedor vive pero su worker no arrancó— y antes
        # no se podía ni representar.
        hace = visto and visto.get("ultimoPedidoHaceMs")
        d["consumiendo"] = consume(hace)
        d["ultimoPedidoHaceMs"] = hace
        salida.append(d)
    return salida, cola


# --- El servidor HTTP ------------------------------------------------------

class Manejador(BaseHTTPRequestHandler):
    server_version = "balanceador/3.0"
    protocol_version = "HTTP/1.1"

    # ¿Se envuelve la respuesta en el sobre del contrato? Sí en el plano
    # público (ManejadorPublico), no en el de control: /admin/backends lo
    # consume el CD, que no es un cliente del servicio.
    sobre = False

    def log_message(self, *_):
        pass  # el registro lo lleva la bitácora, con el formato del contrato

    # -- utilidades --

    def responder(self, codigo, cuerpo):
        if self.sobre:
            cuerpo = {"Code": codigo, "contenido": cuerpo}
        crudo = json.dumps(cuerpo, ensure_ascii=False).encode()
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(crudo)))
        self.end_headers()
        self.wfile.write(crudo)

    def cuerpo_json(self):
        largo = int(self.headers.get("Content-Length") or 0)
        if not largo:
            return {}
        try:
            return json.loads(self.rfile.read(largo).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None

    def descartar_cuerpo(self):
        """Vacía el cuerpo del socket antes de contestar sin haberlo mirado.

        Con keep-alive, los bytes que no se leen quedan en el buffer y el
        servidor los toma como la línea de pedido de la request siguiente: la
        conexión queda envenenada y el próximo pedido del mismo cliente muere
        con un 400 que no tiene nada que ver con lo que mandó.
        """
        largo = int(self.headers.get("Content-Length") or 0)
        if largo:
            self.rfile.read(largo)

    def admin_permitido(self):
        ip = self.client_address[0]
        if ADMIN_PERMITIDOS:
            return ip in ADMIN_PERMITIDOS
        return ip in ("127.0.0.1", "::1")

    def paso(self, operacion, codigo, respuesta, destino, detalle):
        bitacora(operacion, codigo, destino, detalle)
        self.responder(codigo, respuesta)

    # -- el contrato público --
    #
    # Los handlers quedaron finos a propósito: validan lo que el cliente mandó,
    # nombran la operación y dejan pasar el contenido que armó la réplica. Toda
    # la decisión de quién atiende y qué se reintenta está en la cola.

    def identidad(self):
        codigo, contenido, destino, detalle = derivar("GET /", cliente=self.client_address[0])
        if codigo:
            return self.paso("GET /", codigo, contenido, destino, detalle)
        self.paso("GET /", 200, contenido, destino,
                  f"host={contenido.get('host', '?')} {detalle}")

    def salud(self):
        """La salud del servicio entero, no la de una réplica.

        Contesta una sola pregunta: **¿el servicio puede atender ahora mismo?**
        Y para eso hacen falta dos cosas: que la cola esté viva y que haya al
        menos una réplica consumiendo de ella. Sin cola no se atiende nada
        aunque las cuatro réplicas estén perfectas, y una réplica registrada y
        sana cuyo worker no arrancó tampoco atiende nada.

        `replicasSanas` sigue saliendo, pero como información y no como
        veredicto: es lo que el CD mira después de un deploy.
        """
        backends, cola = backends_json()
        sanos = sum(1 for b in backends if b["sano"])
        pedidos = (cola or {}).get("pedidos", {})

        # Quién atiende de verdad se cuenta desde la cola y no desde el registro,
        # y es el número que decide el código HTTP. Con los workers afuera, una
        # réplica atiende porque consume, no porque esté anotada acá ni porque
        # conteste el health gRPC: puede estar consumiendo sin que el CD la haya
        # registrado todavía, o registrada y sana con el worker muerto. Contestar
        # 503 mientras el servicio devuelve 200 sería peor que no tener /health.
        consumiendo = sum(1 for c in (cola or {}).get("consumidores", {}).values()
                          if consume(c.get("ultimoPedidoHaceMs")))
        
        instancias = CLIENTE.instancias()
        master_url = CLIENTE.master_conocido()
        master_inst = next((i for i in instancias if i["url"] == master_url), None) if master_url else None

        if not instancias or all(i.get("rol") == "caido" for i in instancias):
            estado_cola = "caída"
        elif master_inst and master_inst.get("rol") == "master":
            estado_cola = "sana" if cola is not None else "eligiendo"
        elif any(i.get("rol") == "master" for i in instancias):
            estado_cola = "sana" if cola is not None else "eligiendo"
        else:
            estado_cola = "eligiendo"

        rol_master = master_inst.get("rol") if master_inst else ("master" if cola is not None else None)
        termino_master = master_inst.get("termino", 0) if master_inst else 0

        codigo = 200 if (cola is not None and consumiendo) else 503
        if cola is None:
            estado = "sin cola"
        elif not consumiendo:
            estado = "sin réplicas consumiendo"
        else:
            estado = "sano"
        bitacora("GET /health", codigo, None,
                 f"consumiendo={consumiendo} sanos={sanos}/{len(backends)} "
                 f"cola={'sí' if cola else 'NO'} encolados={pedidos.get('esperando', '?')}")
        self.responder(codigo, {
            "balanceador": estado,
            "casa": CASA,
            "replicasConsumiendo": consumiendo,
            "replicasSanas": sanos,
            "replicasTotales": len(backends),
            "cola": {
                "url": CLIENTE.url,
                "rol": rol_master,
                "termino": termino_master,
                "estado": estado_cola,
                "instancias": instancias,
                "encolados": pedidos.get("esperando"),
                "enVuelo": pedidos.get("enVuelo"),
                "cota": pedidos.get("cota"),
                "reasignados": pedidos.get("reasignados"),
                "respuestasPendientes": (cola or {}).get("respuestas", {}).get("pendientes"),
            },
            # Se repiten arriba por compatibilidad: el verificador y la consola
            # del CD ya leían `encolados` y `cota` al ras del contenido.
            "encolados": pedidos.get("esperando"),
            "cota": pedidos.get("cota"),
            "backends": backends,
        })

    def echo(self):
        cuerpo = self.cuerpo_json()
        if cuerpo is None:
            return self.paso("POST /echo", 400, {"error": "cuerpo no es JSON"}, None, None)
        codigo, contenido, destino, detalle = derivar(
            "POST /echo", {"ping": str(cuerpo.get("ping", ""))}, cliente=self.client_address[0])
        self.paso("POST /echo", codigo or 200, contenido, destino, detalle)

    def listar(self):
        codigo, contenido, destino, detalle = derivar("GET /personas",
                                                      cliente=self.client_address[0])
        if codigo:
            return self.paso("GET /personas", codigo, contenido, destino, detalle)
        self.paso("GET /personas", 200, contenido, destino,
                  f"n={len(contenido.get('personas') or [])} {detalle}")

    def crear(self):
        cuerpo = self.cuerpo_json()
        if cuerpo is None:
            return self.paso("POST /personas", 400, {"error": "cuerpo no es JSON"}, None, None)
        # El legajo puede venir como string desde curl; lo normalizamos acá para
        # que la réplica reciba siempre un entero y la validación del contrato
        # falle por lo que tiene que fallar.
        try:
            legajo = int(cuerpo.get("legajo") or 0)
        except (TypeError, ValueError):
            return self.paso("POST /personas", 400,
                             {"error": "legajo tiene que ser un entero"}, None, None)
        # La única escritura: idempotente=False. Si la réplica no contestó a
        # tiempo no sabemos si la creó, y repetirla puede duplicar la persona.
        # La cola respeta esta marca: un pedido no idempotente cuya reserva vence
        # se falla, no se reasigna.
        codigo, contenido, destino, detalle = derivar(
            "POST /personas", {"nombre": str(cuerpo.get("nombre", "")), "legajo": legajo},
            idempotente=False, cliente=self.client_address[0])
        if codigo:
            return self.paso("POST /personas", codigo, contenido, destino, detalle)
        persona = contenido.get("persona") or {}
        self.paso("POST /personas", 201, contenido, destino,
                  f"id={persona.get('id', '?')} {detalle}")

    # -- el endpoint privado que usa el CD para conmutar blue/green --

    def admin_leer(self):
        if not self.admin_permitido():
            return self.paso("GET /admin/backends", 403, {"error": "no autorizado"}, None,
                             f"ip={self.client_address[0]}")
        self.responder(200, {"backends": backends_json()[0]})

    def admin_escribir(self):
        """Conmutación: {"agregar": [...], "quitar": [...]}.

        Cada elemento puede ser "host:puerto" (se asume una réplica Python) o un
        objeto {"destino": "...", "app": "..."}. El CD manda la forma larga: con
        la corta, una réplica Java entraría al registro etiquetada como Python y
        /health mentiría.

        Ojo con lo que este POST hace y lo que ya no hace. Antes agregar una
        réplica le arrancaba workers y quitarla le cortaba el tráfico. Ahora sólo
        cambia el **registro**: quien deja de atender es el worker de la réplica
        cuando baja su contenedor. El deploy tiene que apagar el blue igual que
        antes; conmutar acá es lo que hace que /health cuente lo que hay.
        """
        cuerpo = self.cuerpo_json()   # antes del 403: hay que vaciar el socket igual
        if not self.admin_permitido():
            return self.paso("POST /admin/backends", 403, {"error": "no autorizado"}, None,
                             f"ip={self.client_address[0]}")
        if cuerpo is None:
            return self.paso("POST /admin/backends", 400, {"error": "cuerpo no es JSON"}, None, None)

        agregados, quitados = [], []
        for x in cuerpo.get("agregar") or []:
            destino, app = normalizar_destino(x)
            if not destino:
                continue
            backend = POOL.agregar(destino, app)
            if backend:
                # Se lo chequea acá mismo en vez de esperar al vigilante: el
                # deploy conmuta y consulta /health enseguida, y una réplica
                # recién agregada que todavía figura como no sana haría que
                # /health conteste 503 aunque el servicio esté andando bien.
                backend.chequear()
                agregados.append(destino)
        for x in cuerpo.get("quitar") or []:
            destino, _ = normalizar_destino(x)
            if destino and POOL.quitar(destino):
                quitados.append(destino)

        bitacora("POST /admin/backends", 200, None,
                 f"agregados={agregados or '-'} quitados={quitados or '-'}")
        self.responder(200, {
            "agregados": agregados,
            "quitados": quitados,
            "backends": backends_json()[0],
        })


class ManejadorPublico(Manejador):
    """El puerto que ve el mundo. No sabe qué es /admin: ahí devuelve 404.

    Todo lo que sale de acá va envuelto en {"Code": …, "contenido": {…}}.
    """

    sobre = True

    def do_GET(self):
        ruta = self.path.split("?")[0].rstrip("/") or "/"
        if ruta == "/":
            self.identidad()
        elif ruta == "/health":
            self.salud()
        elif ruta == "/personas":
            self.listar()
        else:
            self.paso(f"GET {ruta}", 404, {"error": "no existe"}, None, None)

    def do_POST(self):
        ruta = self.path.split("?")[0].rstrip("/") or "/"
        if ruta == "/echo":
            self.echo()
        elif ruta == "/personas":
            self.crear()
        else:
            self.descartar_cuerpo()
            self.paso(f"POST {ruta}", 404, {"error": "no existe"}, None, None)


class ManejadorAdmin(Manejador):
    """El plano de control. Sólo lo alcanza el CD, por loopback."""

    def do_GET(self):
        if self.path.split("?")[0].rstrip("/") == "/admin/backends":
            return self.admin_leer()
        self.responder(404, {"error": "no existe"})

    def do_POST(self):
        if self.path.split("?")[0].rstrip("/") == "/admin/backends":
            return self.admin_escribir()
        self.descartar_cuerpo()
        self.responder(404, {"error": "no existe"})


def main():
    # "host:puerto" o "host:puerto=java". Sin la etiqueta se asume python. Hace
    # falta poder decirlo porque tras un reinicio el registro se rearma desde
    # acá: una réplica Java que vuelve etiquetada python miente en /health justo
    # cuando hay que mostrar que el sistema atiende con los dos lenguajes.
    for entrada in (x.strip() for x in BACKENDS_INICIALES.split(",")):
        if not entrada:
            continue
        destino, _, app = entrada.partition("=")
        POOL.agregar(destino.strip(), app.strip() or "python")

    threading.Thread(target=vigilar_salud, daemon=True).start()
    for n in range(RECOLECTORES):
        threading.Thread(target=recolectar, name=f"recolector-{n + 1}", daemon=True).start()

    # Un chequeo antes de abrir el puerto: si las réplicas ya están arriba, la
    # primera request se atiende bien en vez de rebotar con 503 esperando al
    # primer ciclo del vigilante.
    for b in POOL.todos():
        b.chequear()

    admin = ThreadingHTTPServer((ADMIN_BIND, PUERTO_ADMIN), ManejadorAdmin)
    admin.daemon_threads = True
    threading.Thread(target=admin.serve_forever, daemon=True).start()

    servidor = ThreadingHTTPServer(("0.0.0.0", PUERTO), ManejadorPublico)
    servidor.daemon_threads = True
    bitacora("arranque", "OK", None,
             f"publico=0.0.0.0:{PUERTO} admin={ADMIN_BIND}:{PUERTO_ADMIN} "
             f"cola={CLIENTE.url} identidad={IDENTIDAD} recolectores={RECOLECTORES} "
             f"presupuesto={PRESUPUESTO}s "
             f"backends={[b.destino for b in POOL.todos()] or '(vacío)'}")
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        bitacora("apagado", "OK", None, "SIGINT")
        servidor.shutdown()
        admin.shutdown()


if __name__ == "__main__":
    main()
