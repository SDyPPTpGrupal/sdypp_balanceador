#!/usr/bin/env python3
"""Balanceador de la App — HTTP afuera, gRPC adentro.

Es la única URL pública del servicio. Recibe HTTP/JSON (el contrato del
enunciado: GET /, GET /health, POST /echo, GET /personas, POST /personas),
elige una réplica del pool y le hace el RPC correspondiente de contrato.proto.

    python3 app/balanceador.py

    BA_PUERTO=8080 BA_BACKENDS=salvador:8080,mateon:8080 python3 app/balanceador.py

Por qué traduce en vez de reenviar bytes: si sólo pasáramos paquetes (proxy L4)
no sabríamos qué operación pasó, y el enunciado pide una línea de bitácora por
request diciendo a quién se la derivamos. Para escribir esa línea hay que
entender el pedido. El precio es que el balanceador conoce el contrato.
"""

import json
import os
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import contrato_pb2 as pb
import contrato_pb2_grpc as pb_grpc
from cola import Cola, Pedido, Worker

# --- Configuración ---------------------------------------------------------
# Todo por entorno: la topología se decide el día de la demo sin tocar código.

PUERTO = int(os.environ.get("BA_PUERTO", "8080"))
CASA = os.environ.get("BA_CASA", "casa-tomas")
NOMBRE = os.environ.get("BA_NOMBRE", "balanceador")

# Backends iniciales, separados por coma: "salvador:8080,mateon:8080".
# Puede quedar vacío: el deploy.sh los va cargando por /admin/backends.
BACKENDS_INICIALES = os.environ.get("BA_BACKENDS", "")

DIRECTORIO_LOGS = os.environ.get("BA_LOGS", "logs")
ARCHIVO_BITACORA = os.path.join(DIRECTORIO_LOGS, f"bitacora-{NOMBRE}-{CASA}.log")

# Cada cuánto le preguntamos a cada réplica si sigue viva, y cuántas respuestas
# malas seguidas hacen falta para sacarla. Más de una porque un timeout aislado
# es normal en una red doméstica: sacar una réplica sana por un hipo de red
# cuesta más que atender una request de más contra una que ya murió.
INTERVALO_SALUD = float(os.environ.get("BA_INTERVALO_SALUD", "3"))
FALLOS_PARA_SACAR = int(os.environ.get("BA_FALLOS_PARA_SACAR", "2"))
EXITOS_PARA_VOLVER = int(os.environ.get("BA_EXITOS_PARA_VOLVER", "1"))

# Presupuesto total de un pedido, espera en cola incluida. Un solo número y no
# "timeout de cola + timeout de RPC": lo que le importa al cliente es cuánto
# tarda la respuesta, no en qué parte del camino se fue el tiempo.
PRESUPUESTO = float(os.environ.get("BA_TIMEOUT_RPC", "5"))
TIMEOUT_SALUD = float(os.environ.get("BA_TIMEOUT_SALUD", "2"))

# La cola: cuántos pedidos pueden esperar (pasado eso, 503 en el acto) y
# cuántos hilos worker atienden cada réplica, que es lo mismo que decir cuántos
# pedidos en vuelo puede tener cada una.
COTA_COLA = int(os.environ.get("BA_COTA_COLA", "100"))
WORKERS_POR_REPLICA = int(os.environ.get("BA_WORKERS_POR_REPLICA", "4"))

# --- El plano de control, separado del plano de datos -----------------------
# /admin/backends decide a dónde va TODO el tráfico del servicio: quien lo toca
# manda el tráfico a donde quiera. Por eso no vive en el puerto público sino en
# un socket propio, y ese socket escucha por defecto sólo en loopback: lo que no
# escucha en la red no se puede atacar desde la red.
#
# Ese default no alcanza para la demo. Cada casa corre su propio deploy.sh y
# avisa desde su máquina, así que hay que abrirlo: BA_ADMIN_BIND lo hace
# alcanzable y BA_ADMIN_IPS decide quién entra. Las dos cosas, no una.
PUERTO_ADMIN = int(os.environ.get("BA_PUERTO_ADMIN", "8081"))
ADMIN_BIND = os.environ.get("BA_ADMIN_BIND", "127.0.0.1")

# Segunda barrera: qué IPs pueden usarlo, ya habiendo llegado al socket.
# Vacío = sólo loopback.
ADMIN_PERMITIDOS = [x.strip() for x in os.environ.get("BA_ADMIN_IPS", "").split(",") if x.strip()]


def _ahora_iso():
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


_LOCK_BITACORA = threading.Lock()


def bitacora(operacion, codigo, destino=None, detalle=None):
    """Una línea por request, diciendo a quién se la derivamos.

    Mismo formato que la bitácora de las réplicas, a propósito: es lo que
    permite tomar un alta del verificador y seguirla por dos archivos en dos
    casas distintas. Acá queda a quién derivamos; allá, qué hizo esa réplica.
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
    """Una réplica. Mantiene el canal gRPC abierto, su estado de salud y sus workers."""

    def __init__(self, destino, app="python"):
        self.destino = destino
        self.app = app
        self.sano = False          # arranca caído: entra a rotación cuando el
        self.fallos = 0            # chequeo lo confirma, no por confiar en él
        self.exitos = 0
        self.visto = None
        # Un canal por backend, reusado en todas las requests. gRPC multiplexa
        # varias llamadas sobre la misma conexión HTTP/2: abrir uno por request
        # tiraría a la basura el handshake y el arranque lento de TCP.
        self.canal = grpc.insecure_channel(destino)
        self.stub = pb_grpc.ServicioStub(self.canal)
        self.stub_salud = health_pb2_grpc.HealthStub(self.canal)
        # Los hilos que le hablan a esta réplica. Los contadores que muestra
        # /health (en vuelo, atendidos) se suman de acá: cada worker escribe
        # sólo los suyos, así nadie comparte un contador entre hilos.
        self.workers = []
        self._vivos = 0
        self._lock = threading.Lock()

    def atar(self, worker):
        with self._lock:
            self.workers.append(worker)
            self._vivos += 1
        worker.start()

    def soltar(self, worker):
        """Un worker se fue. El último que sale cierra el canal.

        Mientras quede otro, puede tener un RPC en vuelo sobre esa misma
        conexión: cerrarla antes le cortaría la respuesta a un cliente, justo
        durante el cambio de versión, que es cuando se quita una réplica.
        """
        with self._lock:
            self._vivos -= 1
            ultimo = self._vivos <= 0
        if ultimo:
            self.cerrar()

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
        # Con candado porque ahora lo alimentan dos: el vigilante y los workers
        # que se encuentran con la réplica caída en medio de un pedido.
        with self._lock:
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

    def estado_worker(self):
        """Los N workers resumidos en uno: si alguno está ocupado, está ocupada."""
        estados = {w.estado for w in self.workers}
        return next((e for e in Worker.ESTADOS if e in estados), None)

    def como_json(self):
        return {
            "destino": self.destino,
            "app": self.app,
            "sano": self.sano,
            "fallos": self.fallos,
            "ultimoChequeo": self.visto,
            "worker": self.estado_worker(),
            "enVuelo": sum(1 for w in self.workers if w.estado == "ocupado"),
            "atendidos": sum(w.atendidos for w in self.workers),
        }


class Pool:
    """La lista de backends. Ya no reparte: cada réplica tiene workers que
    sacan de la cola cuando pueden, y el reparto sale solo de eso.

    Sigue habiendo un candado, pero ahora protege el dict: agregar y quitar
    réplicas pasa por el plano de control mientras los workers preguntan
    `tiene()` todo el tiempo.
    """

    def __init__(self, cola):
        self._lock = threading.Lock()
        self._backends = {}
        self._cola = cola

    def agregar(self, destino, app="python"):
        """Devuelve el backend recién creado, o None si el destino ya estaba.

        Devuelve el objeto y no un booleano para que quien lo agrega pueda
        chequearlo en el acto, sin esperar al próximo ciclo del vigilante.
        Arranca sus workers: hasta que el chequeo lo dé por sano, duermen.
        """
        with self._lock:
            if destino in self._backends:
                return None
            backend = Backend(destino, app)
            self._backends[destino] = backend
        for n in range(1, WORKERS_POR_REPLICA + 1):
            backend.atar(Worker(backend, self._cola, self, n))
        return backend

    def quitar(self, destino):
        """Sólo lo saca de la lista. No cierra nada: sus workers ven que ya no
        está, terminan lo que tienen en vuelo y el último cierra el canal."""
        with self._lock:
            return self._backends.pop(destino, None) is not None

    def tiene(self, backend):
        """¿Este objeto sigue en el pool? Por identidad, no por destino: si se
        quita y se vuelve a agregar el mismo destino, es otro Backend con otros
        workers, y los viejos tienen que irse."""
        with self._lock:
            return self._backends.get(backend.destino) is backend

    def fallo(self, backend):
        """Un worker no pudo ni conectarse. Es el mismo contador que usa el
        vigilante: la caída que detecta un pedido y la que detecta el chequeo
        periódico son la misma cosa, sólo que una llega antes."""
        if backend._registrar(False):
            bitacora("salud", "CAMBIO", backend.destino, "sale de rotación (falló una request)")

    def todos(self):
        with self._lock:
            return list(self._backends.values())


COLA = Cola(COTA_COLA)
POOL = Pool(COLA)


def vigilar_salud():
    """Le pregunta a cada réplica cada INTERVALO_SALUD segundos.

    Preguntando y no esperando a que una request falle: si esperáramos al fallo,
    cada muerte le costaría un error a un usuario real. Igual reaccionamos al
    fallo también (ver Worker.atender), porque entre dos chequeos hay una ventana.
    """
    while True:
        for b in POOL.todos():
            try:
                if b.chequear():
                    estado = "vuelve a rotación" if b.sano else "sale de rotación"
                    bitacora("salud", "CAMBIO", b.destino, estado)
            except Exception as e:
                print(f"[salud] {b.destino}: {e}", flush=True)
        time.sleep(INTERVALO_SALUD)


# --- Traducción gRPC <-> HTTP ---------------------------------------------

CODIGOS = {
    grpc.StatusCode.OK: 200,
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.ALREADY_EXISTS: 409,
    grpc.StatusCode.PERMISSION_DENIED: 403,
    grpc.StatusCode.UNAUTHENTICATED: 401,
    grpc.StatusCode.DEADLINE_EXCEEDED: 504,
    grpc.StatusCode.UNAVAILABLE: 503,
    grpc.StatusCode.UNIMPLEMENTED: 501,
}

def persona_json(p):
    return {"id": p.id, "nombre": p.nombre, "legajo": p.legajo}


def _destino(pedido):
    return pedido.intentos[-1] if pedido.intentos else None


def _camino(pedido):
    """`intentos=a→b ` cuando pasó por más de una réplica: la evidencia de la
    reasignación, en la misma línea de bitácora que el resultado."""
    return f"intentos={'→'.join(pedido.intentos)} " if len(pedido.intentos) > 1 else ""


def derivar(operacion, llamar, idempotente=True, cliente=None):
    """Encola el pedido y espera a que un worker lo atienda.

    `llamar(stub, timeout, metadata)` hace el RPC contra el stub que le den:
    quién lo atiende lo decide el worker que lo saque, no este hilo. La regla
    de reintento vive en Worker.atender; acá sólo se espera el resultado.

    Devuelve (codigo_http, cuerpo, destino, detalle). codigo_http es None si
    salió bien y `cuerpo` es la respuesta del RPC.
    """
    if not POOL.todos():
        # Sin réplicas no hay worker que vaya a sacar nada: esperar el
        # presupuesto entero para contestar 504 sería mentirle al cliente.
        return 503, {"error": "no hay réplicas en el pool"}, None, "pool vacío"

    pedido = Pedido(operacion, llamar, idempotente,
                    vence_en=time.monotonic() + PRESUPUESTO, cliente=cliente)
    req = f"req={pedido.request_id}"
    if not COLA.put(pedido):
        return (503, {"error": "cola llena"}, None,
                f"cola llena ({COLA.largo()}/{COLA.cota}) {req}")

    if not pedido.listo.wait(timeout=max(pedido.queda(), 0)):
        # Nadie lo atendió a tiempo: todas caídas o todas lentas. El pedido
        # queda en la cola; el worker que lo saque lo va a descartar sin RPC.
        return (504, {"error": "sin respuesta a tiempo"}, _destino(pedido),
                f"venció {_camino(pedido)}{req}")

    codigo, r = pedido.resultado
    if codigo == grpc.StatusCode.OK:
        return None, r, _destino(pedido), f"{_camino(pedido)}{req}"
    return (CODIGOS.get(codigo, 500), {"error": r}, _destino(pedido),
            f"{codigo.name} {_camino(pedido)}{req}")


# --- El servidor HTTP ------------------------------------------------------

class Manejador(BaseHTTPRequestHandler):
    server_version = "balanceador/2.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass  # el registro lo lleva la bitácora, con el formato del contrato

    # -- utilidades --

    def responder(self, codigo, cuerpo):
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

    def admin_permitido(self):
        ip = self.client_address[0]
        if ADMIN_PERMITIDOS:
            return ip in ADMIN_PERMITIDOS
        return ip in ("127.0.0.1", "::1")

    def paso(self, operacion, codigo, respuesta, destino, detalle):
        bitacora(operacion, codigo, destino, detalle)
        self.responder(codigo, respuesta)

    # -- el contrato público --

    # Los lambda no eligen réplica: reciben el stub, el tiempo que queda y la
    # metadata del worker que los saque de la cola. El cliente viaja como
    # x-forwarded-for para que la réplica sepa quién pidió de verdad.

    def identidad(self):
        codigo, r, destino, detalle = derivar("GET /", lambda s, timeout, metadata: s.Identidad(
            pb.IdentidadPedido(), timeout=timeout, metadata=metadata),
            cliente=self.client_address[0])
        if codigo:
            return self.paso("GET /", codigo, r, destino, detalle)
        self.paso("GET /", 200, {
            "app": r.app,
            "lenguaje": r.lenguaje,
            "equipo": [{"nombre": i.nombre, "apellido": i.apellido, "legajo": i.legajo}
                       for i in r.equipo],
            "version": r.version,
            "mensaje": r.mensaje,
            "host": r.host,
            "arrancado": r.arrancado,
            "servidoPor": r.app,
        }, destino, f"host={r.host} {detalle}")

    def salud(self):
        """La salud del balanceador, no la de una réplica.

        Devuelve 200 mientras haya alguien en rotación: un 200 acá significa
        "el servicio puede atender", que es lo que le importa a quien pregunta.
        """
        backends = [b.como_json() for b in POOL.todos()]
        sanos = sum(1 for b in backends if b["sano"])
        codigo = 200 if sanos else 503
        bitacora("GET /health", codigo, None, f"sanos={sanos}/{len(backends)}")
        self.responder(codigo, {
            "balanceador": "sano" if sanos else "sin réplicas",
            "casa": CASA,
            "replicasSanas": sanos,
            "replicasTotales": len(backends),
            "backends": backends,
        })

    def echo(self):
        cuerpo = self.cuerpo_json()
        if cuerpo is None:
            return self.paso("POST /echo", 400, {"error": "cuerpo no es JSON"}, None, None)
        codigo, r, destino, detalle = derivar("POST /echo", lambda s, timeout, metadata: s.Echo(
            pb.PingPedido(ping=str(cuerpo.get("ping", ""))), timeout=timeout, metadata=metadata),
            cliente=self.client_address[0])
        if codigo:
            return self.paso("POST /echo", codigo, r, destino, detalle)
        self.paso("POST /echo", 200,
                  {"pong": r.pong, "servidoPor": r.servido_por, "version": r.version},
                  destino, detalle)

    def listar(self):
        codigo, r, destino, detalle = derivar("GET /personas", lambda s, timeout, metadata: s.ListarPersonas(
            pb.ListarPersonasPedido(), timeout=timeout, metadata=metadata),
            cliente=self.client_address[0])
        if codigo:
            return self.paso("GET /personas", codigo, r, destino, detalle)
        self.paso("GET /personas", 200,
                  {"servidoPor": r.servido_por, "personas": [persona_json(p) for p in r.personas]},
                  destino, f"n={len(r.personas)} {detalle}")

    def crear(self):
        cuerpo = self.cuerpo_json()
        if cuerpo is None:
            return self.paso("POST /personas", 400, {"error": "cuerpo no es JSON"}, None, None)
        # El legajo puede venir como string desde curl; lo normalizamos acá para
        # que el stub no lo rechace antes de llegar a la validación del contrato.
        try:
            legajo = int(cuerpo.get("legajo") or 0)
        except (TypeError, ValueError):
            return self.paso("POST /personas", 400,
                             {"error": "legajo tiene que ser un entero"}, None, None)
        # La única escritura: idempotente=False. Si la réplica no contestó a
        # tiempo no sabemos si la creó, y repetirla puede duplicar la persona.
        codigo, r, destino, detalle = derivar("POST /personas", lambda s, timeout, metadata: s.CrearPersona(
            pb.NuevaPersona(nombre=str(cuerpo.get("nombre", "")), legajo=legajo),
            timeout=timeout, metadata=metadata),
            idempotente=False, cliente=self.client_address[0])
        if codigo:
            return self.paso("POST /personas", codigo, r, destino, detalle)
        self.paso("POST /personas", 201,
                  {"servidoPor": r.servido_por, "persona": persona_json(r.persona)},
                  destino, f"id={r.persona.id} {detalle}")

    # -- el endpoint privado que usa el deploy.sh de cada casa --

    def admin_leer(self):
        if not self.admin_permitido():
            return self.paso("GET /admin/backends", 403, {"error": "no autorizado"}, None,
                             f"ip={self.client_address[0]}")
        self.responder(200, {"backends": [b.como_json() for b in POOL.todos()]})

    def admin_escribir(self):
        """Conmutación: {"agregar": [...], "quitar": [...]}.

        Cada elemento puede ser "host:puerto" (se asume una réplica Python) o un
        objeto {"destino": "...", "app": "..."}. La forma corta es la que ya
        manda el deploy.sh, así que ese script no se toca.

        Primero se agrega y después se quita, en ese orden: al revés hay un
        instante con menos réplicas en rotación de las que debería.
        """
        if not self.admin_permitido():
            return self.paso("POST /admin/backends", 403, {"error": "no autorizado"}, None,
                             f"ip={self.client_address[0]}")
        cuerpo = self.cuerpo_json()
        if cuerpo is None:
            return self.paso("POST /admin/backends", 400, {"error": "cuerpo no es JSON"}, None, None)

        def normalizar(x):
            if isinstance(x, str):
                return x, "python"
            if isinstance(x, dict) and x.get("destino"):
                return x["destino"], x.get("app", "python")
            return None, None

        agregados, quitados = [], []
        for x in cuerpo.get("agregar") or []:
            destino, app = normalizar(x)
            if not destino:
                continue
            backend = POOL.agregar(destino, app)
            if backend:
                # Se lo chequea acá mismo en vez de esperar al vigilante: el
                # deploy conmuta y consulta /health enseguida, y una réplica
                # recién agregada que todavía figura como no sana haría que
                # /health conteste 503 aunque el tráfico se esté atendiendo bien.
                backend.chequear()
                agregados.append(destino)
        for x in cuerpo.get("quitar") or []:
            destino, _ = normalizar(x)
            if destino and POOL.quitar(destino):
                quitados.append(destino)

        bitacora("POST /admin/backends", 200, None,
                 f"agregados={agregados or '-'} quitados={quitados or '-'}")
        self.responder(200, {
            "agregados": agregados,
            "quitados": quitados,
            "backends": [b.como_json() for b in POOL.todos()],
        })


class ManejadorPublico(Manejador):
    """El puerto que ve el mundo. No sabe qué es /admin: ahí devuelve 404."""

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
            self.paso(f"POST {ruta}", 404, {"error": "no existe"}, None, None)


class ManejadorAdmin(Manejador):
    """El plano de control. Sólo lo alcanzan los deploy.sh de las casas."""

    def do_GET(self):
        if self.path.split("?")[0].rstrip("/") == "/admin/backends":
            return self.admin_leer()
        self.responder(404, {"error": "no existe"})

    def do_POST(self):
        if self.path.split("?")[0].rstrip("/") == "/admin/backends":
            return self.admin_escribir()
        self.responder(404, {"error": "no existe"})


def main():
    # "host:puerto" o "host:puerto=java". Sin la etiqueta se asume python, que es
    # como venía. Hace falta poder decirlo porque tras un reinicio el pool se
    # rearma desde acá: una réplica Java que vuelve etiquetada python miente en
    # /health y en la auditoría justo cuando hay que mostrar que reparte entre
    # los dos lenguajes.
    for entrada in (x.strip() for x in BACKENDS_INICIALES.split(",")):
        if not entrada:
            continue
        destino, _, app = entrada.partition("=")
        POOL.agregar(destino.strip(), app.strip() or "python")

    threading.Thread(target=vigilar_salud, daemon=True).start()

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
             f"backends={[b.destino for b in POOL.todos()] or '(vacío)'}")
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        bitacora("apagado", "OK", None, "SIGINT")
        servidor.shutdown()
        admin.shutdown()


if __name__ == "__main__":
    main()
